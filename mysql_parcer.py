import json
from ddl_parsing import DdlParse
import logging
import traceback

#configure the logging details   
logging.basicConfig(filename='log.txt',level=logging.INFO,format='%(asctime)s:%(levelname)s:%(message)s')

def parsing(sample_ddl):
    
    try:
        parser = DdlParse(sample_ddl)
        table,schema_name = parser.parse()
        dict1=table.to_bigquery_fields()
     
        
        #print(dict1)

        final_array=[]
        final_array.append(schema_name)
        for col in table.columns.values():
            col_info = {}
            
            
            col_info["name"]                  = col.name
            col_info["data_type"]             = col.data_type
            col_info["length"]                = col.length
            col_info["precision(=length)"]    = col.precision
            col_info["scale"]                 = col.scale
            col_info["is_unsigned"]           = col.is_unsigned
            col_info["is_zerofill"]           = col.is_zerofill
            col_info["constraint"]            = col.constraint
            col_info["not_null"]              = col.not_null
            col_info["PK"]                    = col.primary_key
            col_info["FK"]                    = col.foreign_key
            col_info["unique"]                = col.unique
            col_info["auto_increment"]        = col.auto_increment
            col_info["distkey"]               = col.distkey
            col_info["sortkey"]               = col.sortkey
            col_info["encode"]                = col.encode
            col_info["default"]               = col.default
            col_info["character_set"]         = col.character_set
            col_info["bq_legacy_data_type"]   = col.bigquery_legacy_data_type
            col_info["bq_standard_data_type"] = col.bigquery_standard_data_type
            col_info["comment"]               = col.comment
            col_info["description(=comment)"] = col.description
            col_info["bigquery_field"]        = json.loads(col.to_bigquery_field())


            final_array.append(col_info)
        
        # logging.info("json for"+str(sample_ddl))
        logging.info(json.dumps(final_array, indent=2, ensure_ascii=False))
    except:
        traceback.print_exc()

if __name__=="__main__":
    
    try:
        with open('pf_fk_table_dump','r') as file:
            ddl = file.read()
            parsing(ddl)
    except Exception as e:
        logging.info("Error while uploading file {}".format(e))
    
    
    



    