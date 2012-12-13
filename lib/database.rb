begin
  require 'rubygems'
rescue LoadError
  puts "'rubygems' nao encontrado."
  exit
end

begin
  require 'pg'
rescue LoadError
  puts "'pg' nao encontrado. É necessário 'pg' >= 0.14.1"
  puts "Se desejar, pode instalar a gem utilizando o arquivo que se encontra na pasta 'gems'"
  exit
end

begin
  require 'yaml'
rescue LoadError
  puts "'yaml' nao encontrado."
  exit
end

class Database

  def initialize
    @logger = Log.new
    @connection = get_connection
  end

  def connect(host, port, db, user, pw)
    PGconn.new(host, port, '', '', db, user, pw)
  end

  def get_connect
    @connection
  end

  def get_connection
    config = YAML::load(File.open('config/postgres.yml'))
    begin
      conn = connect(config['host'], config['port'], config['database'],config['user'],config['password'])
      @logger.info "Conectado no banco #{conn.db} em #{conn.host}"
    rescue PGError=>e
      @logger.info "Não foi possível conectar ao banco. [#{e}]"
    end
    @logger.close
    conn
  end

  def close
    @connection.close unless @connection.nil?
    @logger.info "Conexão terminada."
    @logger.close
  end

end