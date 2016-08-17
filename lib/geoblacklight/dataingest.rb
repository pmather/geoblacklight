require 'csv'
require 'digest'
require 'fileutils'
require 'json'
require 'net/http'
require 'uri'
require 'yaml'

class DataIngest
    
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
    else
      return ""
    end
  end


  def solrdatahash(content)
    rec = {}
    uuid = Digest::SHA256.hexdigest content[1] 
    timestring = Time.now().strftime("%Y-%m-%dT%H:%M:%SZ") 

    rec['uuid'] = uuid
    rec['dc_identifier_s'] = content[1]
    rec['dc_rights_s'] = content[2]
    rec['dct_provenance_s'] = content[3]
    if !content[4].nil?
      rec['dct_references_s'] = "{\"http://schema.org/downloadUrl\":\"" + content[1] + "\"}"
    else
      rec['dct_references_s'] = "{\"http://schema.org/downloadUrl\":\"" + content[1] + "\",\"http://www.opengis.net/def/serviceType/ogc/wcs\":\"" + content[4] + "\"}"     
    end
    rec['dc_creator_sm'] = content[5]
    rec['dc_language_s'] = content[6]
    rec['dc_publisher_s'] = content[7]
    rec['dc_type_s'] = content[9]
    rec['dct_spatial_sm'] = content[10]
    rec['dct_temporal_sm'] = content[11]
    rec['dct_issued_s'] = timestring
    rec['dct_isPartOf_sm'] = content[13]
    rec['georss_box_s'] = content[14]
    if !content[14].nil?
      gdata = content[14].split(",")  
      rec['solr_geom'] = "ENVELOPE("+ gdata[1] + "," + gdata[3] + "," + gdata[2] + "," + gdata[0] + ")"
    end
    rec['dc_title_s'] = content[17]
    rec['dc_description_s'] = content[18]
    rec['dc_format_s'] = content[19]
    rec['dc_subject_sm'] = content[20]
    rec['layer_id_s'] = content[21]
    rec['layer_modified_dt'] = timestring
    rec['layer_slug_s'] = content[23]
    rec['layer_geom_type_s'] = content[24]

    return rec
  end

  def is_number? string
    true if Float(string) rescue false
  end


  def is_valid_url(urlstring)
    uri = URI.parse(urlstring)
    if uri.kind_of?(URI::HTTP) or uri.kind_of?(URI::HTTPS)
        return true
      else
        return false
    end
  end


  def validaterecord(content)
    result = ""
    fields = ["uuid","dc:identifier", "dc:rights", "dct:provenance", "dct:references",
          "dc:creator", "dc:language", "dc:publisher", "dc:relation", 
          "dc:type", "dct:spatial", "dct:temporal", "dct:issued", 
          "isPartOf", "georss:box", "georss:point", "georss:polygon",
          "dc:title","dc:description","dc:format","dc:subject",
          "layer:id","layer:modified","layer:slug","layer:geom_type"]
    emptystr = " is empty. "

    for i, item in content.each_with_index.map {|x, i| [i, x]}
      if item[1].nil? and ![0, 8, 11, 12, 15, 16, 20, 22].include? i
        result += fields[i] + emptystr  
      elsif i == 1 and !is_valid_url(item[1])
        result += "dc_identifier field is not a valid URL. "
      elsif i == 14 and content[14].split(",").length != 4
        result += "georss_box field is incorrect. "
      elsif i == 14 and !content[14].split(",").all? {|i| is_number?( i ) }
        result += "georss_box field should be all numbers. "  
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
        CSV.foreach(File.join(uploadpath, uploadfile), :headers => true) do |row|
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
        logfilename = File.join(logpath, prefix + Time.now().strftime("%Y%m%d%H%M%S").to_s + ".log.txt")
        logcontent = uploadfile + ": Total ingest records: " + totalrecs.to_s + ", ingested " + ingestedrecs.to_s + " records."
        createreport(logfilename, logcontent)

        # create error report file
        if totalrecs != ingestedrecs
          errorfilename = File.join(errorpath, prefix + Time.now().strftime("%Y%m%d%H%M%S").to_s + ".error.txt")
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