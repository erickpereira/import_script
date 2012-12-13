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

ftp = FTP.new
if ftp.need_do_import?
  import = ImportFile.new
  import.do_import
end
