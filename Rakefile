# 
# To change this template, choose Tools | Templates
# and open the template in the editor.

require 'rubygems'
require 'rake'

begin
  require 'lib/ftp'
rescue LoadError
  puts "'lib/ftp' nao encontrado."
  exit
end

begin
  require 'lib/import_file'
rescue LoadError
  puts "'lib/import_file' nao encontrado."
  exit
end

namespace :content do
  namespace :import do
    desc "importa os dados do ftp da fenabrave na base de dados do abracam"
    task :data do
      ftp = FTP.new
      if ftp.need_do_import?
        import = ImportFile.new
        import.do_import
      end
    end
  end
end


