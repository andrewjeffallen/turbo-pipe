import jinja2
import turbodbc
import numpy as np
import pandas as pd
import time
from turbodbc import make_options, connect


class SqlServer:
    def __init__(self, uid, driver, server, database, pwd, port=1433):
        """[summary]

        Args:
            uid ([type]): [description]
            driver ([type]): [description]
            server ([type]): [description]
            database ([type]): [description]
            pwd ([type]): [description]
            port (int, optional): [description]. Defaults to 1433.
        """
        try:
            self.uid = uid
            self.driver = driver
            self.server = server
            self.database = database
            self.port = port
            self.pwd = pwd

            self._connection = turbodbc.connect(
                turbodbc_options=turbodbc.make_options(prefer_unicode=True),
                UID=self.uid,
                driver=self.driver,
                server=self.server,
                database=self.database,
                PWD=self.pwd,
            )

        except Exception as e:
            raise Exception(
                "__init__(): SqlServer {},{}, db {} failed with exception while initializing: {}".format(
                    self.server, str(self.port), self.database, str(e)
                )
            )

    def sql_to_df(self, template_file, path, **context):
        """[summary]

        Args:
            sql_file ([type]): [description]
        """
        template_loader = jinja2.FileSystemLoader(searchpath=f"{path}/")
        template_env = jinja2.Environment(loader=template_loader)
        template = template_env.get_template(template_file)
        rendered_file = template.render(**context)
        
        sql_as_string = rendered_file
        df = pd.read_sql_query(sql_as_string, self._connection)
        return df

    def turbo_write_sql(self, df, target, truncate=False):
        """[summary]

        Args:
            df (dataframe): pandas dataframe to be loaded into SQL Server
            table (str): target table name in SQL Server
            truncate (bool, optional): Truncates table prior to load if set to True. Defaults to False.

        Returns:
            None
        """        
        df = df.fillna(value=np.nan)
        start = time.time()
        # preparing columns
        columns = "("
        columns += ", ".join(df.columns)
        columns += ")"

        # preparing value place holders
        val_place_holder = ["?" for col in df.columns]
        sql_val = "("
        sql_val += ", ".join(val_place_holder)
        sql_val += ")"

        # writing sql query for turbodbc
        sql = f"""
        INSERT INTO {self.database}.dbo.{target} {columns}
        VALUES {sql_val}
        """

        # writing array of values for turbodbc
        values = [
            np.ma.MaskedArray(df[col].values, pd.isnull(df[col].values))
            for col in df.columns
        ]

        print(sql)

        if truncate:
            print("Truncating table before load")
            # cleans the previous head insert
            with self._connection.cursor() as cursor:
                try:
                    cursor.execute(f"delete from {self.database}.dbo.{target}")
                    self._connection.commit()
                    print("Truncate succesful")
                except Exception as e:
                    raise(e)
        else:
            pass

        # insert values from dataframe into table
        with self._connection.cursor() as cursor:
            try:
                cursor.executemanycolumns(sql, values)
                self._connection.commit()
                print(f"Loaded {len(df)} rows to {self.database}.dbo.{target}")
            except Exception as e:
                self._connection.rollback()
                raise(e)

        stop = time.time() - start
        return print(f"Load time: {stop} seconds")
