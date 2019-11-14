import pymysql
import csv


def loadCSVMYSQL(curDate):
    # create and load data from CSV to Mysql
    if curDate == '': return []
    csvRuncard = ['runcard_A.csv','runcard_B.csv']
    con = pymysql.connect(host='localhost', user='root', passwd='aaa', db='CSV')
    try:
        with con.cursor() as cursor :
        # create tables Processes, Runcard, Schedule_Runcard, Loads
            createRunCardProcessTables(cursor)
            createLoadTable(cursor)
        # insert data from CSV to MySQL tables
            for fileCSV in csvRuncard:
                with open(fileCSV,'r') as csvRun:
                    RunCardAData = csv.DictReader(csvRun)
                    insertRunCardProcessTables(cursor, RunCardAData, fileCSV.strip('.csv'))
            with open('loads.csv', 'r') as csvLoads :
                loadData = csv.DictReader(csvLoads)
                insertLoadTable(cursor,loadData)
            # build report about actual task for date
            res = selectTaskCurDate(cursor, curDate )
        con.commit()
    finally:
        con.close()
    return res

def insertRunCardProcessTables(cursor,RunCardAData,nameTypeProgram):
    cursor.execute("INSERT  INTO `CSV`.`Runcard` (`name_runcard`) VALUES (%s) ON DUPLICATE KEY UPDATE name_runcard = name_runcard",
                   (nameTypeProgram));

    for line in RunCardAData:
        cursor.execute("INSERT  INTO `CSV`.`Processes` (`name_process`,`passive_step`) VALUES (%s,%s) ON DUPLICATE KEY"
                       " UPDATE name_process = name_process",
                       (line['process'], 1 if line['process'] == "Colonize" else 0));

        cursor.execute("INSERT INTO `CSV`.`Schedule_Runcard` (`day`, `id_process`, `id_runcard`)"
                       "VALUES(%s, (SELECT p.id FROM `CSV`.`Processes` p WHERE name_process = %s),"
                     "(SELECT r.id FROM `CSV`.`Runcard`r WHERE name_runcard = %s))",
                       (line['day'],line['process'],nameTypeProgram))


def insertLoadTable (cursor, loadData) :
    for line in loadData:
        cursor.execute("INSERT INTO `CSV`.`loads` (`bed_id`, `load_date`, `id_runcard`)"
                       " VALUES(%s,  %s, (SELECT r.id FROM `CSV`.`Runcard`r WHERE name_runcard = %s))",
                       (line['bed_id'],line['load_date'],line['runcard_type'] ))

def createLoadTable(cursor):
    cursor.execute("""CREATE TABLE IF NOT EXISTS `CSV`.`loads` (
                                   `bed_id` INT NOT NULL,
                                  load_date` DATETIME,
                                    `id_runcard` INT NOT NULL,
                                   PRIMARY KEY (`bed_id`),
                                    CONSTRAINT `fk_id_runcard`
                                    FOREIGN KEY (`id_runcard`)
                                    REFERENCES `CSV`.`Runcard` (`id`))""")

def createRunCardProcessTables(cursor):

    cursor.execute("""CREATE TABLE IF NOT EXISTS `CSV`.`Processes` (
                                  `id` INT NOT NULL AUTO_INCREMENT,
                                  `name_process` VARCHAR(45),
                                  `passive_step` BOOLEAN,
                                  UNIQUE INDEX `name_process_UNIQUE` (`name_process` ASC) VISIBLE,
                                  PRIMARY KEY (`id`) )""")
    cursor.execute("""CREATE TABLE IF NOT EXISTS `CSV`.`Runcard` (
                   `id` INT NOT NULL AUTO_INCREMENT,
                   `name_runcard` VARCHAR(45),
                   UNIQUE INDEX `nname_runcard_UNIQUE` (`name_runcard` ASC) VISIBLE,
                   PRIMARY KEY (`id`) )""")
    cursor.execute("""CREATE TABLE IF NOT EXISTS `CSV`.`Schedule_Runcard` (
                                   `id` INT NOT NULL AUTO_INCREMENT,
                                  `day` INT,
                                    `id_runcard` INT NOT NULL,
                                    `id_process` INT NOT NULL,
                                   PRIMARY KEY (`id`),
                                    CONSTRAINT `fkid_process`
                                    FOREIGN KEY (`id_process`)
                                    REFERENCES `CSV`.`Processes` (`id`),
                                   CONSTRAINT `fkid_runcard`
                                    FOREIGN KEY (`id_runcard`)
                                    REFERENCES `CSV`.`Runcard` (`id`))""")


def selectTaskCurDate(cursor, curDate):

    query = ("""SELECT res.bed_id , 
    Runcard.name_runcard, 
    proc.name_process 
    FROM (SELECT * FROM
    (SELECT l.bed_id, l.load_date, 
    ADDDATE(l.load_date, ifnull(s.day,0)) curDate, 
    l.id_runcard, 
    s.id_process 
    FROM `CSV`.`loads` l 
    LEFT JOIN   `CSV`.`Schedule_Runcard` s ON s.id_runcard =  l.id_runcard ) res2  
    WHERE res2.curDate = DATE(%s) ) res 
    INNER JOIN  `CSV`.`Runcard` Runcard ON res.id_runcard = Runcard.id 
    INNER JOIN `CSV`.`Processes` proc ON res.id_process = proc.id 
    WHERE  proc.passive_step = 0 
    ORDER BY res.bed_id, Runcard.name_runcard, proc.name_process""")

    cursor.execute(query, (curDate))
    columns = cursor.fetchall()
    return columns

if __name__ == '__main__':
    dataAnalysis = '2019-10-06'
    res = loadCSVMYSQL(dataAnalysis)


