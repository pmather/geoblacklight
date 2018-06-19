require 'csv'
require 'digest'
require 'fileutils'
require 'json'
require 'net/http'
require 'uri'
require 'yaml'

class DataIngest

  @@fields = {
    "dc:identifier": {required: true},
    "dc:rights": {required: true},
    "dct:provenance": {required: true},
    "dct:references": {required: false},
    "dc:creator": {required: true},
    "dc:language": {required: true},
    "dc:publisher": {required: true},
    "dc:type": {required: true},
    "dct:spatial": {required: true},
    "dct:temporal": {required: false},
    "dct:issued": {required: false},
    "ispartof": {required: true},
    "solr:geom": {required: true},
    "georss:polygon": {required: false},
    "dc:title": {required: true},
    "dc:description": {required: true},
    "dc:format": {required: true},
    "dc:subject": {required: false},
    "layer:id": {required: true},
    "layer:modified": {required: false},
    "layer:slug": {required: true},
    "layer:geom_type": {required: true}
  }
    
  def createreport(filename, content)
    File.open(filename, 'w') { |file| file.write(content) }
  end


  def getfilelist(folderpath)
    return Dir[folderpath].select{ |filename| File.file? filename }.map{ |filename| File.basename filename }
  end


  def getfileprefix(filename)
    if filename.start_with?("dmf_")
      return "dmf_"
    elsif filename.start_with?("cgit_")
      return "cgit_"
    elsif filename.start_with?("gdrive_")
      return "gdrive_"
    else
      return ""
    end
  end


  def solrdatahash(content)
    rec = {} 
    timestring = Time.now().strftime("%Y-%m-%dT%H:%M:%SZ") 

    rec['dc_identifier_s'] = content[0]
    rec['dc_rights_s'] = content[1]
    rec['dc_title_s'] = content[2]
    rec['dct_provenance_s'] = content[3]
    if !content[17].nil?
      rec['dct_references_s'] = "{\"http://schema.org/downloadUrl\":\"" + content[0] + "\",\"http://www.opengis.net/def/serviceType/ogc/wcs\":\"" + content[17] + "\"}"
    end
    rec['layer_slug_s'] = content[5]
    if !content[6].nil?
      gdata = content[6].split(",")
      rec['solr_geom'] = "ENVELOPE("+ gdata[1] + "," + gdata[3] + "," + gdata[2] + "," + gdata[0] + ")"
    end
    rec['dc_creator_sm'] = content[7]
    rec['dc_description_s'] = content[8]
    rec['dc_format_s'] = content[9]
    rec['dc_language_sm'] = content[10]
    rec['dc_publisher_sm'] = content[11]
    rec['dc_subject_sm'] = content[13]
    rec['dc_type_s'] = content[14]
    rec['dct_isPartOf_sm'] = content[15]
    rec['dct_issued_s'] = timestring
    rec['dct_spatial_sm'] = content[18]
    rec['dct_temporal_sm'] = content[19]
    rec['layer_geom_type_s'] = content[20]
    rec['layer_id_s'] = content[21]
    rec['layer_modified_dt'] = timestring

    return rec
  end

  def is_number? string
    true if Float(string) rescue false
  end


  def is_valid_url(urlstring)
    begin
      uri = URI.parse(urlstring)
      if uri.kind_of?(URI::HTTP) or uri.kind_of?(URI::HTTPS)
        return true
      else
        return false
      end
    rescue
      return false
    end
  end


  def validaterecord(row)
    if row.length === 0
      return "Row does not contain record."
    end

    result = ""
    row.each do |key, content|
      begin
        item = /<(.+)>/.match(key).captures[0].downcase rescue ""

        if @@fields.key?(item.to_sym) && @@fields[item.to_sym][:required] && content.blank?
          result += "#{item} is empty. "

        elsif item == "dc:identifier" && !is_valid_url(content)
          result += "dc_identifier field is not a valid URL. "
      
        elsif item == "solr:geom" && (!content.respond_to?("split") || content.split(",").length != 4)
          result += "solr_geom field is incorrect. "

        elsif item == "solr:geom" && (!content.respond_to?("split") || !content.split(",").all? {|i| is_number?( i ) })
          result += "solr_geom field should be all numbers. "
        end
      rescue
        result += "unknown error parsing row. "
      end
    end

    return result
  end

  def dirMissing dir
    marker = "===================================\n"
    error = "#{dir} does not exist. Halting."
    message = marker + Time.now.inspect + " - " + error + "\n" + marker
    Rails.logger.error message
    raise IOError
  end

  def run
    basepath = Rails.application.secrets.ingest_dir || "/opt/sftp/geodata"
    archivepath = File.join(basepath, "Archive")
    reportpath = File.join(basepath, "Report")
    errorpath = File.join(reportpath, "Errors")
    logpath = File.join(reportpath, "Logs")
    uploadpath = File.join(basepath, "Upload")

    dirMissing(uploadpath) unless Dir.exists?(uploadpath)
    
    list_of_files = getfilelist(File.join(uploadpath, "*"))
    for uploadfile in list_of_files

      prefix = getfileprefix(uploadfile)
      if prefix.length > 0
        
        errorcontent = ""
        totalrecs = 0
        ingestedrecs = 0
        index = 1
        CSV.foreach(File.join(uploadpath, uploadfile), headers: true, encoding: 'iso-8859-1:utf-8' ) do |row|
          errmsg = validaterecord(row)

          if errmsg.length > 0
            errorcontent += "row" + index.to_s + ":" + errmsg + "\n\n"
          else
            solrdata = solrdatahash(row)
            Blacklight.default_index.connection.add(solrdata)
            Blacklight.default_index.connection.commit
            ingestedrecs += 1
          end

          totalrecs += 1
          index += 1
        end

        # create log report file
        logfilename = File.join(logpath, uploadfile + "_" + Time.now().strftime("%Y%m%d%H%M%S").to_s + ".log.txt")
        logcontent = uploadfile + ": Total ingest records: " + totalrecs.to_s + ", ingested " + ingestedrecs.to_s + " records."
        createreport(logfilename, logcontent)

        # create error report file
        if totalrecs != ingestedrecs
          errorfilename = File.join(errorpath, uploadfile + "_" + Time.now().strftime("%Y%m%d%H%M%S").to_s + ".error.txt")
          createreport(errorfilename, uploadfile + "\n" + errorcontent)
        end

        # move upload file to archive folder
        src = File.join(uploadpath, uploadfile)
        dest = File.join(archivepath, uploadfile)
        FileUtils.mv(src, dest)
      
      end

    end
    
  end

end
