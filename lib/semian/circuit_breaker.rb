# frozen_string_literal: true

module Semian
  class CircuitBreaker # :nodoc:
    extend Forwardable

    def_delegators :@state, :closed?, :open?, :half_open?

    attr_reader(
      :name,
      :half_open_resource_timeout,
      :error_timeout,
      :state,
      :last_error,
      :error_threshold_timeout_enabled,
    )

    def initialize(name, exceptions:, success_threshold:, error_threshold:,
      error_timeout:, implementation:, half_open_resource_timeout: nil,
      error_threshold_timeout: nil, error_threshold_timeout_enabled: true, dryrun:)

      @name = name.to_sym
      @success_count_threshold = success_threshold
      @error_count_threshold = error_threshold
      @error_threshold_timeout = error_threshold_timeout || error_timeout
      @error_threshold_timeout_enabled = error_threshold_timeout_enabled.nil? ? true : error_threshold_timeout_enabled
      @error_timeout = error_timeout
      @exceptions = exceptions
      @half_open_resource_timeout = half_open_resource_timeout
      @dryrun = dryrun

      @errors = implementation::Error.new
      @successes = implementation::Integer.new
      @state = implementation::State.new

      reset
    end

    # Conditions to check with dryrun
    # In open state should not calle mark_failed, mark_success.
    # mark_success should only be called during half_open state.

    def acquire(resource = nil, &block)
      transition_to_half_open if transition_to_half_open?

      unless request_allowed?
        if @dryrun
          Semian.logger.info("Throwing Open Circuit Error")
        else
          raise OpenCircuitError
        end
      end

      result = nil
      begin
        result = maybe_with_half_open_resource_timeout(resource, &block)
      rescue *@exceptions => error
        if !error.respond_to?(:marks_semian_circuits?) || error.marks_semian_circuits?
          mark_failed(error) unless open?
        end
        raise error
      else
        mark_success unless open?
      end
      result
    end

    def transition_to_half_open?
      open? && error_timeout_expired? && !half_open?
    end

    def request_allowed?
      closed? || half_open? || transition_to_half_open?
    end

    def mark_failed(error)
      push_error(error)
      Semian.logger.info("Marking resource failure in Semian - #{_error.class.name} : #{_error.message}")
      @errors.increment
      set_last_error_time
      if closed?
        transition_to_open if error_threshold_reached?
      elsif half_open?
        transition_to_open
      end
    end

    def mark_success
      return unless half_open?

      @errors.reset
      @successes.increment
      transition_to_close if success_threshold_reached?
    end

    def reset
      @errors.reset
      @successes.reset
      transition_to_close
    end

    def destroy
      @errors.destroy
      @successes.destroy
      @state.destroy
    end

    def in_use?
      !error_timeout_expired? && !@errors.empty?
    end

    private

    def transition_to_close
      notify_state_transition(:closed)
      log_state_transition(:closed, Time.now)
      @state.close!
      @errors.reset
      @successes.reset
    end

    def transition_to_open
      notify_state_transition(:open)
      log_state_transition(:open, Time.now)
      @state.open!
    end

    def transition_to_half_open
      notify_state_transition(:half_open)
      log_state_transition(:half_open, Time.now)
      @state.half_open!
      @errors.reset
      @successes.reset
    end

    def success_threshold_reached?
      @successes.value >= @success_count_threshold
    end

    def error_threshold_reached?
      @errors.value >= @error_count_threshold
    end

    def error_timeout_expired?
      return false unless @errors.last_error_time
      Time.at(@errors.last_error_time) + @error_timeout < Time.now
    end

    def push_error(error)
      @last_error = error
    end

    def set_last_error_time(time: Time.now)
      @errors.last_error_at(time.to_i)
    end

    def log_state_transition(new_state, occur_time)
      return if @state.nil? || new_state == @state.value

      str = "[#{self.class.name}] State transition from #{@state.value} to #{new_state} at #{occur_time}."
      str += " success_count=#{@successes.value} error_count=#{@errors.value}"
      str += " success_count_threshold=#{@success_count_threshold}"
      str += " error_count_threshold=#{@error_count_threshold}"
      str += " error_timeout=#{@error_timeout} error_last_at=\"#{@errors.last_error_time ? Time.at(@errors.last_error_time) : ''}\""
      str += " name=\"#{@name}\""
      if new_state == :open && @last_error
        str += " last_error_message=#{@last_error.message.inspect}"
      end

      Semian.logger.info(str)
    end

    def notify_state_transition(new_state)
      Semian.notify(:state_change, self, nil, nil, state: new_state)
    end

    def maybe_with_half_open_resource_timeout(resource, &block)
      if half_open? && @half_open_resource_timeout && resource.respond_to?(:with_resource_timeout)
        resource.with_resource_timeout(@half_open_resource_timeout) do
          block.call
        end
      else
        block.call
      end
    end
  end
end
