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

begin
  require 'iconv'
rescue LoadError
  puts "'iconv' nao encontrado."
  exit
end

class ImportFile

  def initialize
    @logger = Log.new
    if File.exist?("download/emplacamentos.txt")
      @file = File.open("download/emplacamentos.txt","r")
    else
      @logger.info "Não existe arquivo para importação no diretório download"
    end
  end

  def to_utf8(value)
    Iconv.conv('utf-8','ISO-8859-1', value)
  end

  def do_import
    @logger = Log.new
    @logger.info "Obtendo dados do arquivo de importação..."
    data = data_hash
    @logger.info "Dados obtidos com sucesso. Iniciando importação..."
    database = Database.new
    index = 0
    data.each do |emplacamento|
      ########## EMPRESA ##########
      if emplacamento[:cnpj]
        empresa = database.get_connect.exec("SELECT * FROM empresas WHERE cnpj = '#{emplacamento[:cnpj]}'").first
        # se não encontrou, utiliza a razão social caso a razão social seja o cnpj
        if empresa
          @empresa_id = empresa["id"]
        else
          @logger.info "Linha #{index+1}: Não foi encontrada a empresa informada [#{emplacamento[:cnpj]}]"
          empresa = database.get_connect.exec("SELECT * FROM empresas WHERE cnpj = '#{emplacamento[:cnpj].gsub(',','').gsub('/','').gsub('=','').gsub(')','')}'").first
          if empresa
            @empresa_id = empresa["id"]
          else # se ainda não encontrou, adiciona nova empresa
            begin
              database.get_connect.exec("INSERT INTO empresas VALUES (nextval('empresas_id_seq'), '#{emplacamento[:razao_social]}','#{emplacamento[:cnpj]}', true);")
            rescue PGError=>e
              @logger.info "Linha #{index+1}: Não foi possível inserir registro de empresa [#{emplacamento[:razao_social]} - #{emplacamento[:cnpj]}] - #{e.inspect}"
            ensure
              @logger.info "Linha #{index+1}: Empresa (#{emplacamento[:razao_social]},#{emplacamento[:cnpj]}) criada com sucesso."
              @empresa_id = database.get_connect.exec("SELECT * FROM empresas_id_seq").first['last_value']
            end
          end
        end
      end
      
      ########## FABRICANTE ##########
      if emplacamento[:fabricante_codigo]
        fabricante = database.get_connect.exec("SELECT * FROM fabricantes WHERE id = '#{emplacamento[:fabricante_codigo]}'").first

        if fabricante
          @fabricante_id = fabricante["id"]
        else
          @logger.info "Linha #{index+1}: Não foi encontrada o fabricante informado [#{emplacamento[:fabricante]}]"
          begin
            database.get_connect.exec("INSERT INTO fabricantes VALUES ('#{emplacamento[:fabricante_codigo]}', '#{emplacamento[:fabricante]}');")
          rescue PGError=>e
            @logger.info "Linha #{index+1}: Não foi possível inserir registro de fabricante [#{emplacamento[:fabricante_codigo]} - #{emplacamento[:fabricante]}] - #{e.inspect}"
          ensure
            @logger.info "Linha #{index+1}: Fabricante [#{emplacamento[:fabricante_codigo]},#{emplacamento[:fabricante]}] criado com sucesso."
            @fabricante_id = database.get_connect.exec("SELECT * FROM fabricantes_id_seq").first['last_value']
          end
        end
      end

      ########## FAMILIA ##########
      if emplacamento[:grupo_modelo_veiculo_codigo]
        familia = database.get_connect.exec("SELECT * FROM familias WHERE id = #{emplacamento[:grupo_modelo_veiculo_codigo]}").first

        if familia
          @familia_id = familia["id"]
        else
          @logger.info "Linha #{index+1}: Não foi encontrada a família informada [#{emplacamento[:grupo_modelo_veiculo]}]"
          begin
            database.get_connect.exec("INSERT INTO familias VALUES ('#{emplacamento[:grupo_modelo_veiculo_codigo]}', '#{emplacamento[:grupo_modelo_veiculo]}', '#{@fabricante_id}');") if @fabricante_id
            @logger.info "Não existe fabricante definido para adicionar uma família." unless @fabricante_id
          rescue
            @logger.info "Linha #{index+1}: Não foi possivel inserir registro de fabricante [#{emplacamento[:grupo_modelo_veiculo_codigo]}-#{emplacamento[:grupo_modelo_veiculo]}]"
          ensure
            @logger.info "Linha #{index+1}: Familia [#{emplacamento[:grupo_modelo_veiculo_codigo]},#{emplacamento[:grupo_modelo_veiculo]}] criada com sucesso."
            @familia_id = database.get_connect.exec("SELECT * FROM familias_id_seq").first['last_value']
          end
        end
      end

      ########## SUB-SEGMENTO ##########
      if emplacamento[:sub_segmento_codigo]
        sub_segmento = database.get_connect.exec("SELECT * FROM sub_segmentos WHERE id = #{emplacamento[:sub_segmento_codigo]}").first

        if sub_segmento
          @sub_segmento_id = sub_segmento["id"]
        else
          @logger.info "Linha #{index+1}: Não foi encontrada o sub-segmento informado [#{emplacamento[:sub_segmento]}]"
          begin
            database.get_connect.exec("INSERT INTO sub_segmentos VALUES ('#{emplacamento[:sub_segmento_codigo]}', '#{emplacamento[:sub_segmento]}');")
          rescue
            @logger.info "Linha #{index+1}: Não foi possível inserir registro de sub-segmento [#{emplacamento[:sub_segmento_codigo]}-#{emplacamento[:sub_segmento]}]"
          ensure
            @logger.info "Linha #{index+1}: Sub-Segmento [#{emplacamento[:sub_segmento_codigo]},#{emplacamento[:sub_segmento]}] criado com sucesso."
            @sub_segmento_id = database.get_connect.exec("SELECT * FROM sub_segmentos_id_seq").first['last_value']
          end
        end
      end

      ########## MODELO ##########
      if emplacamento[:modelo_codigo]
        modelo = database.get_connect.exec("SELECT * FROM modelos WHERE id = '#{emplacamento[:modelo_codigo]}'").first

        if modelo
          @modelo_id = modelo["id"]
        else
          @logger.info "Linha #{index+1}: Não foi encontrado o modelo informado [#{emplacamento[:modelo]}]"
          begin
            database.get_connect.exec("INSERT INTO modelos (id, nome, familia_id, sub_segmento_id, emplacamentos_count) VALUES ('#{emplacamento[:modelo_codigo]}', '#{emplacamento[:modelo]}', '#{@familia_id}', '#{@sub_segmento_id}', 1);") if @sub_segmento_id and @familia_id
            @logger.info "Não existe família definida para inserir este modelo" unless @familia_id
            @logger.info "Não existe sub-segmento definido para inserir este modelo" unless @sub_segmento_id
          rescue
            @logger.info "Linha #{index+1}: Não foi possível inserir registro de sub-segmento [#{emplacamento[:modelo_codigo]}-#{emplacamento[:modelo]}]"
          ensure
            @logger.info "Linha #{index+1}: Modelo [#{emplacamento[:modelo_codigo]},#{emplacamento[:modelo]}] criado com sucesso."
            @modelo_id = database.get_connect.exec("SELECT * FROM modelos_id_seq").first['last_value']
          end
        end
      end

      ########## CIDADE ##########
      if emplacamento[:municipio_codigo]
        cidade = database.get_connect.exec("SELECT * FROM cidades WHERE id = '#{emplacamento[:municipio_codigo]}'").first

        if cidade
          @cidade_id = cidade["id"]
          if (cidade["estado_id"].nil? or cidade["estado_id"].empty?) and emplacamento[:estado]
            estado = database.get_connect.exec("SELECT * FROM estados WHERE sigla = '#{emplacamento[:estado]}'").first
            if estado
              begin
                database.get_connect.exec("UPDATE cidades SET estado_id = '#{estado["id"]}' WHERE id = '#{cidade["id"]}'")
              rescue
                @logger.info "Linha #{index+1}: Não foi possível atualizar a cidade [#{estado['id']},#{emplacamento[:municipio]}]"
              ensure
                @logger.info "Cidade atualizada com sucesso [#{estado['id']},#{emplacamento[:municipio]}]"
                @estado_id = estado["id"]
              end
            else
              @logger.info "Linha #{index+1}: Não foi encontrado um estado com os dados informados. [#{emplacamento[:estado]}]"
            end
          end
        else
          if emplacamento[:estado]
            estado = database.get_connect.exec("SELECT * FROM estados WHERE sigla = '#{emplacamento[:estado]}'")
            if estado
              begin
                database.get_connect.exec("INSERT INTO cidades VALUES ('#{emplacamento[:municipio_codigo]}', '#{estado["id"]}', '#{emplacamento[:municipio]}', TRUE);")
              rescue
                @logger.info "Linha #{index+1}: Não foi possível inserir uma cidade [#{emplacamento[:municipio_codigo]},#{emplacamento[:municipio]}, #{emplacamento[:estado]}]"
              ensure
                @logger.info "Linha #{index+1}: Estado não encontrado: [#{emplacamento[:estado]}]"
                @cidade_id = database.get_connect.exec("SELECT * FROM cidades_id_seq").first['last_value']
                @estado_id = estado["id"]
              end
            else
              begin
                database.get_connect.exec("INSERT INTO cidades VALUES ('#{emplacamento[:municipio_codigo]}', null, '#{emplacamento[:municipio]}', TRUE);")
              rescue
                @logger.info "Não foi possível inserir uma cidade sem estado [#{emplacamento[:municipio_codigo]},#{emplacamento[:municipio]}]"
              ensure
                @logger.info "Linha #{index+1}: Estado não encontrado: [#{emplacamento[:estado]}]"
                @cidade_id = database.get_connect.exec("SELECT * FROM cidades_id_seq").first['last_value']
              end
            end
          end
        end
      end

      ########## COMBUSTIVEL ##########
      if emplacamento[:combustivel_codigo]
        combustivel = database.get_connect.exec("SELECT * FROM combustiveis WHERE id = '#{emplacamento[:combustivel_codigo]}'").first
        if combustivel
          @combustivel_id = combustivel["id"]
        else
          begin
            database.get_connect.exec("INSERT INTO combustiveis VALUES ('#{emplacamento[:combustivel_codigo]}', '#{emplacamento[:combustivel]}');")
          rescue
            @logger.info "Linha #{index+1}: Não foi possível inserir um registro de combustível [#{emplacamento[:combustivel_codigo]}-#{emplacamento[:combustivel]}]"
          ensure
            @logger.info "Combustivel criado com sucesso.[#{emplacamento[:combustivel_codigo]},#{emplacamento[:combustivel]}]"
            @conbustivel_id = database.get_connect.exec("SELECT * FROM combustiveis_id_seq").first['last_value']
          end
        end
      end

      ########## EMPLACAMENTO ##########
      data_emplacamento = emplacamento[:data_emplacamento].gsub(" 00:00:00","")
      data_emplacamento = data_emplacamento.split("/").reverse.join("-")

      #verificando se tem o emplacamento ja inserido no banco
      emplacamento_from_db = database.get_connect.exec("SELECT * FROM emplacamentos WHERE chassi = '#{emplacamento[:numero_chassis]}' AND data = '#{data_emplacamento}' AND placa = '#{emplacamento[:numero_placa]}'").first
      unless emplacamento_from_db
        begin
          query = "INSERT INTO emplacamentos (data,empresa_id,chassi,placa,modelo_id,sub_segmento_id,cidade_id,combustivel_id,ativo,created_at,updated_at,ano_fabricacao) VALUES "
          values = [
            "'"+data_emplacamento+"'",
            "'"+(@empresa_id ? @empresa_id : 'NULL')+"'",
            "'"+emplacamento[:numero_chassis]+"'",
            "'"+emplacamento[:numero_placa]+"'",
            "'"+@modelo_id ? @modelo_id : 'NULL'+"'",
            "'"+(@sub_segmento_id ? @sub_segmento_id : 'NULL')+"'",
            "'"+(@cidade_id ? @cidade_id : 'NULL')+"'",
            "'"+(@combustivel_id ? @combustivel_id : 'NULL')+"'",
            "'"+'true'+"'",
            "'"+Time.now.strftime("%Y-%m-%d")+"'",
            "'"+Time.now.strftime("%Y-%m-%d")+"'",
            "'"+emplacamento[:ano_fabricacao]+"'"
          ]
          database.get_connect.exec(query+"(#{values.join(',')})")
        rescue PGError=>e
          @logger.info "[ERRO] Linha #{index+1}: Não foi possível inserir o registro de emplacamento => #{e.inspect}"
        end
      end
      if (index+1) % 500 == 0
        @logger.info "Sucesso linha #{index+1}"
        @logger.close
        @logger = Log.new
      end
      index += 1
    end
    @logger.info "[#{Time.now.strftime("%Y-%m-%d %H:%M")}] Importação finalizada."
  end

  private

  def data_hash
    result = []
    if @file
      @file.each do |line|
        data = line.split(";")
        result << {:cnpj =>                to_utf8(data[0].strip),
          :razao_social =>                 to_utf8(data[1].strip),
          :data_emplacamento =>            to_utf8(data[2].strip),
          :numero_chassis =>               to_utf8(data[3].strip),
          :numero_placa =>                 to_utf8(data[4].strip),
          :fabricante =>                   to_utf8(data[5].strip),
          :fabricante_codigo =>            to_utf8(data[6].strip),
          :grupo_modelo_veiculo =>         to_utf8(data[7].strip),
          :grupo_modelo_veiculo_codigo =>  to_utf8(data[8].strip),
          :modelo =>                       to_utf8(data[9].strip),
          :modelo_codigo =>                to_utf8(data[10].strip),
          :segmento =>                     to_utf8(data[11].strip),
          :segmento_codigo =>              to_utf8(data[12].strip),
          :sub_segmento =>                 to_utf8(data[13].strip),
          :sub_segmento_codigo =>          to_utf8(data[14].strip),
          :municipio =>                    to_utf8(data[15].strip),
          :municipio_codigo =>             to_utf8(data[16].strip),
          :estado =>                       to_utf8(data[17].strip),
          :combustivel =>                  to_utf8(data[18].strip),
          :combustivel_codigo =>           to_utf8(data[19].strip),
          :potencia =>                     to_utf8(data[20].strip),
          :capacidade_carga =>             to_utf8(data[21].strip),
          :capacidade_passageiros =>       to_utf8(data[22].strip),
          :codigo_nacionalidade =>         to_utf8(data[23].strip),
          :ano_fabricacao =>               to_utf8(data[24].strip)
        }
      end
    else
      @logger.info "Não existe arquivo para ser importado"
    end
    result
  end

end