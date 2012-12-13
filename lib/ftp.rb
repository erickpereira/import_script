begin
  require 'yaml'
rescue LoadError
  puts "'yaml' nao encontrado."
  exit
end

begin
  require 'lib/database'
rescue LoadError
  puts "'lib/database' nao encontrado."
  exit
end

begin
  require 'lib/log'
rescue LoadError
  puts "'lib/log' nao encontrado."
  exit
end

class FTP

  SERVER_FILE = "Emplacamentos_Diario_Segmentos_S_Fabricante.txt"
  LOCAL_FILE = "download/emplacamentos.txt"

  def initialize
    @logger = Log.new
    config_file = YAML::load(File.open('config/ftp.yml'))
    get_file(config_file)
    @logger.close
  end

  def config_file=(values)
    @config_file = values
  end

  def config_file
    @config_file
  end

  def need_do_import?
    need_import = false
    database = Database.new
    result = database.get_connection.exec("SELECT emplacamentos.data FROM emplacamentos ORDER BY data DESC LIMIT 1")
    if result and result.first
      result_date = result.first['data']
      if result_date.split(" ")[0] == (Time.now - 60*60*24).strftime("%Y-%m-%d")
        @logger.info "Arquivo atual já foi integrado ao banco de dados!"
      else
        need_import = true
      end
    end
    database.close
    need_import
  end

  def get_file(configurations)
    # usa o LFTP para baixar o arquivo com uma conexao ativa
    # na Amazon eh necessario setar o IP publico para conexoes ativas
    if File.exist?(LOCAL_FILE)
      @logger.info "Arquivo existente de emplacamentos foi apagado com sucesso." if File.delete(LOCAL_FILE)
    end
    result = system("lftp -u #{configurations['user']},#{configurations['password']} -e 'set ftp:port-ipv4 #{configurations['ip']}; set ftp:passive-mode false; set xfer:clobber true; get -c #{SERVER_FILE} -o #{LOCAL_FILE}; quit' #{configurations['url']}")
    if result == false
      @logger.info "Ocorreu um problema ao obter o arquivo do FTP."
    end
  end

end