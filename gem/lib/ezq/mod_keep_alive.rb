module KeepAlive

  def KeepAlive.monitor(num: 1, timeout: 30, log: nil, &block)

    pids = {}

    num.times do |idx|
      cmd = yield(idx)
      log.info "Initial start of #{cmd}" if log
      pid = spawn(cmd)
      pids[pid] = idx
    end

    # Check in on the processes and restart as needed
    loop do
      sleep(timeout)
      pids.to_a.each do |pid,idx|
        exited = Process.wait(pid,Process::WNOHANG)
        if exited
          pids.delete(pid)
          cmd = yield(idx)
          log.info "Detected failure with status '#{$?.to_i}'. Restarting '#{cmd}'" if log
          pid = spawn(cmd)
          pids[pid] = idx
        end
      end
    end
  end

end