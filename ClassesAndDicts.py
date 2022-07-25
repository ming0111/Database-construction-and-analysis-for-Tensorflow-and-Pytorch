import pandas as pd
        
class Schema(object):
    def __init__(self, name):
        self.__name = name
        
    def create_schema(self, cursor):
        cursor.execute(f"CREATE SCHEMA {self.__name};")
        print(cursor.statusmessage, "\n")
        
    def drop_schema_cascade(self, cursor):
        cursor.execute(f"DROP SCHEMA {self.__name} CASCADE;")
        print(cursor.statusmessage, "\n")
        
    def new_table(self, df:pd.DataFrame, fields:str, dtypes:str, tablename:str):
        return Table(fields, df, dtypes, ".".join((self.__name, tablename)))       
    
class Table(object):
    def __init__(self, fields:str, df:pd.DataFrame, dtypes:str, tablename:str):
        self.__fields = fields.split(",")
        self.__dtypes = dtypes.split(",")
        self.__tablename = tablename
        self.__values = Table._get_values_from_df(df[[col for col in self.__fields]])
    
    def rename_table(self, new_tablename: str):
        self.__tablename = new_tablename
    
    def rename_fields(self, new_fields: str):
        self.__fields = new_fields.split(",")
        
    def create_table(self, cursor):
        Table._create(self.__tablename, cursor, self.__fields, self.__dtypes)
        print(cursor.statusmessage, "\n")
    
    def drop_table_cascade(self, cursor):
        cursor.execute(f"DROP TABLE {self.__tablename} CASCADE;")
        print(cursor.statusmessage, "\n")
    
    def insert_into(self, cursor):
        Table._insert(self.__tablename, cursor, self.__fields, self.__values)

    @staticmethod
    def _get_values_from_df(df: pd.DataFrame):
        return list(df.itertuples(index=False, name=None))
    
    @staticmethod
    def _create(tablename, cursor, fields, dtypes):
        sql_input = f"CREATE TABLE {tablename}\n("
        sql_body = ",\n".join(" ".join((field, dtype)) for field, dtype in zip(fields,dtypes))
        cursor.execute("\n".join((sql_input, sql_body, ');')))
    
    
    @staticmethod
    def _insert(tablename, cursor, fields, values):
        sql_head_1 = f"INSERT INTO {tablename}\n("
        sql_body_1 = ",\n".join(fields)
        sql_head_2 = f") VALUES ("
        sql_body_2 = ",\n".join(['%s']*len(fields))
        sql_input = "\n".join((sql_head_1,sql_body_1,sql_head_2,sql_body_2,");"))
        cursor.executemany(sql_input, values)

#define gitIssues tables and datatype
git_issues_data_dict = {
"issues": ("""title,project,body,user_id,closed_by,created_at,updated_at,closed_at,assignees,labels,reactions,n_comments""",
                        """text,text,text,text,text,timestamp,timestamp,timestamp,text,text,text,int"""),
"users": ("user_id,user","text,text"),
"comments": ("""comment_id,user_id,comment_user_id,title,created_at,comment_created_at,comment_updated_at,comment_text""",
             "text,text,text,text,timestamp,timestamp,timestamp,text"),
"comment_user": ("""comment_user_id,comment_user""","""text,text""")
}

#define gitData tables and datatype
git_data_data_dict = {
"commits": ("""hash,msg,author_name,author_date,author_timezone,committer_name,committer_date,committer_timezone,branches,parents,project_name,deletions,insertions,lines,files""",
            """text,text,text,timestamp,text,text,timestamp,text,text,text,text,int,int,int,int"""),
"files": ("hash,old_path,new_path,filename,change_type,diff,diff_parsed,deleted_lines,source_code,source_code_before,nloc,complexity,token_count",
          "text,text,text,text,text,text,text,int,text,text,decimal,decimal,decimal")
} 

connection_params = dict(
dbname = "git",
user = "postgres",
password = "0324",
host = "localhost",
port = "4321") # note my port is 4321, please change if yours is 5432