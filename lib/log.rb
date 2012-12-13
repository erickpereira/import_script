
class Log

  LOG_FILE = "log/log_#{Time.now.strftime("%d%m%Y_%H%M%S")}.txt"

  def initialize
    create_load_log_file
  end

  def info(values)
    if @logger.closed?
      create_load_log_file
      @logger.puts(values)
    else
      @logger.puts(values)
    end
  end

  def close
    @logger.close
  end

  private

  def create_load_log_file
    if File.exist?(LOG_FILE)
      @logger = File.open(LOG_FILE, File::WRONLY | File::APPEND)
    else
      @logger = File.open(LOG_FILE, File::WRONLY | File::APPEND | File::CREAT)
    end
  end

end
