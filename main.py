import pyodbc 
server = 'AYUSH-AHER'
database = 'SchoolERP'
username = 'sa'
password = 'abcdefgh'
connection_string = f'DRIVER={{SQL Server}};SERVER={server};DATABASE={database};UID={username};PWD={password}'

primaryKeyColumnName = "Id"

ignoreColumns = ["CreatedBy",
                "CreatedOn",
                "LastModifiedBy",
                "LastModifiedOn",
                "DeletedBy",
                "DeletedOn",
                primaryKeyColumnName]

cnxn = pyodbc.connect(connection_string)


mycursor = cnxn.cursor()

# path = input("Path of the file where you want to save: ")
path = "D:\\Automation\\SP\\Output\\"

def init():
    mycursor.execute("SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE='BASE TABLE'")
    for x in mycursor.fetchall():
        if not str(x[0]).lower().startswith("vw_"):
            for table in x:
                # print(table)
                # CreateProcedureInsert(table)
                CreateProcedureUpdate(table)
                    


def CreateProcedureInsert(table):
    sp = [
        f"CREATE PROCEDURE [dbo].[sp_{table}_In]\n",
        "@Id int,\n",
        "@UserId int,",

    ]

    mycursor.execute(f"SELECT * FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = N'{table}'")
    lstColumns = mycursor.fetchall()
    # Get All Parameters required
    for column in lstColumns:
       if(column[3] not in ignoreColumns):
            sp.append(f"\n@{column[3]} {column[7]},")
    sp[-1] = sp[-1][:-1]

    sp.append(f"\nAS \n BEGIN \n\t IF NOT EXISTS (SELECT * FROM {table} WHERE \t")
    sp[-1] = sp[-1][:-1]

    for column in lstColumns:
       if(column[3] not in ignoreColumns):
            sp.append(f"{column[3]} = @{column[3]} AND ")
    sp[-1] = sp[-1][:-4] + ") \n\t BEGIN"
    
    sp.append(f"\n \t\t INSERT INTO [dbo].{table} \n\t\t\t(")
    for column in lstColumns:
       if(column[3] not in ignoreColumns):
            sp.append(f"[{column[3]}],")
    sp.append(f"[CreatedBy], [CreatedOn],")

    sp[-1] = sp[-1][:-1] + ")"
    sp.append(f"\n \t\tvalues(")

    for column in lstColumns:
        if(column[3] not in ignoreColumns):
                sp.append(f"@{column[3]},")
    sp.append(f"@UserId, CURRENT_TIMESTAMP,")
    sp[-1] = sp[-1][:-1] + ") \n \t\t RETURN 1 \n\t END \n\t ELSE RETURN 3"

    sp.append("\t END\n\n")
    # wFile = open(f"{path}\\sp.sql", "w")
    file = open(f"{path}\\SP.sql", "a")
    file.writelines(sp)


def CreateProcedureUpdate(table):
    sp = [
        f"CREATE PROCEDURE [dbo].[sp_{table}_Up]\n",
        "@UserId int,",

    ]

    mycursor.execute(f"SELECT * FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = N'{table}'")
    lstColumns = mycursor.fetchall()
    # Get All Parameters required
    for column in lstColumns:
       if(column[3] not in ignoreColumns or column[3] == primaryKeyColumnName):
            sp.append(f"\n@{column[3]} {column[7]},")
    sp[-1] = sp[-1][:-1]

    sp.append(f"\nAS \n BEGIN \n\t IF NOT EXISTS (SELECT * FROM {table} WHERE \t")
    sp[-1] = sp[-1][:-1]

    for column in lstColumns:
       if(column[3] not in ignoreColumns or column[3] == primaryKeyColumnName):
            sp.append(f"{column[3]} = @{column[3]} AND ")
    sp[-1] = sp[-1][:-4] + ") \n\t BEGIN"
    
    sp.append(f"\n \t\t UPDATE [dbo].{table} \n\t\t\tSET \n")
    for column in lstColumns:
       if(column[3] not in ignoreColumns):
            sp.append(f"[{column[3]}] = @{column[3]},")
    sp.append(f"[LastModifiedBy] = @UserId, [LastModifiedOn] = CURRENT_TIMESTAMP,")
    sp[-1] = sp[-1][:-1] + f" WHERE Id = @Id \n \t\t RETURN 2 \n\t END \n\t ELSE RETURN 4"

    sp.append("\t END\n\n")
    # wFile = open(f"{path}\\SP_UP.sql", "w")
    file = open(f"{path}\\SP_Up.sql", "a")
    file.writelines(sp)

if __name__ == '__main__':
    try:
        print("Note: sp.sql and Delete sp.Sql files should be present before executing the code.")
        init()
    except Exception as e:
        print(e)
