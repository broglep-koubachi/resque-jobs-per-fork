require 'resque'
require 'resque/worker'


module Resque

  # the `before_perform_jobs_per_fork` hook will run in the child perform
  # right before the child perform starts
  #
  # Call with a block to register a hook.
  # Call with no arguments to return all registered hooks.
  def self.before_perform_jobs_per_fork(&block)
    block ? register_hook(:before_perform_jobs_per_fork, block) : hooks(:before_perform_jobs_per_fork)
  end

  # Register a before_perform_jobs_per_fork proc.
  def self.before_perform_jobs_per_fork=(block)
    register_hook(:before_perform_jobs_per_fork, block)
  end

  # the `after_perform_jobs_per_fork` hook will run in the child perform
  # right before the child perform terminates
  #
  # Call with a block to set the hook.
  # Call with no arguments to return the hook.
  def self.after_perform_jobs_per_fork(&block)
    block ? register_hook(:after_perform_jobs_per_fork, block) : hooks(:after_perform_jobs_per_fork)
  end

  # Set the after_perform_jobs_per_fork proc.
  def self.after_perform_jobs_per_fork=(block)
    register_hook(:after_perform_jobs_per_fork, block)
  end

  class Worker

    def perform_with_jobs_per_fork(job)
      raise "You need to set JOBS_PER_FORK on the command line" unless ENV['JOBS_PER_FORK']
      run_hook :before_perform_jobs_per_fork, self
      jobs_performed ||= 0
      while jobs_performed < ENV['JOBS_PER_FORK'].to_i do
        break if @shutdown
        if jobs_performed == 0
          perform_without_jobs_per_fork(job)
        elsif another_job = reserve
          perform_without_jobs_per_fork(another_job)
        else
          break # to prevent looping/hammering Redis with LPOPs
        end
        jobs_performed += 1
      end
      jobs_performed = nil
      run_hook :after_perform_jobs_per_fork, self
    end
    alias_method :perform_without_jobs_per_fork, :perform
    alias_method :perform, :perform_with_jobs_per_fork
  end
end
