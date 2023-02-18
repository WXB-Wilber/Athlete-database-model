#实验十六运动员数据库模型
import pandas as pd
import pymysql

#连接数据库
conn = pymysql.connect(host='localhost', user='root', password='123456', database='sport_database')
print("连接成功\n")
cursor = conn.cursor()

##---------------------------------创建sports_database数据库的表格--------------------------------------##

def create_tables():
    # 创建队伍表格
    sql_term = 'create table if not exists term(' \
               'team_number int(10) primary key,' \
               'term_name varchar(50) not null,' \
               'term_area varchar(50) not null)'
    cursor.execute(sql_term)
    print("创建队伍表格成功！")

    # 创建队员表格
    sql_athlete = 'create table if not exists athlete(' \
                  'athlete_number int(10) primary key,' \
                  'athlete_name varchar(50) not null,' \
                  'athlete_sex varchar(50) not null,' \
                  'athlete_age int(10),' \
                  'athlete_term varchar(50) not null)'
    cursor.execute(sql_athlete)
    print("创建队员表格成功！")

    # 创建比赛表格
    sql_competition = 'create table if not exists competition(' \
                      'competition_time datetime,' \
                      'competition_area varchar(50) not null,' \
                      'competition_contestant varchar(50) not null,' \
                      'competition_type varchar(50),' \
                      'competition_result varchar(50) not null)'
    cursor.execute(sql_competition)
    print("创建比赛表格成功！")

    # 创建参赛队员表格
    sql_competitors = 'create table if not exists competitors(' \
                      'competitors_number int(10) primary key ,' \
                      'competitors_name varchar(50) not null,' \
                      'competitors_time datetime)'
    cursor.execute(sql_competitors)
    print("创建参赛队员表格成功！\n")





##-----------------------------------------------插入数据库数据-------------------------------------------##

def insert_data():
    # 插入队伍信息
    insert_term = """insert into term(team_number,term_name,term_area) values \
                (001,'中国国家队','中国'),(002,'阿塞拜疆国家队','阿塞拜疆'), \
                (003,'英格兰国家队','英格兰'), \
                (004,'法国国家队','法国'), \
                (005,'新西兰国家队','新西兰'), \
                (006,'印度尼西亚国家队','印度尼西亚'), \
                (007,'越南国家队','越南')"""
    cursor.execute(insert_term)
    conn.commit()
    print("插入队伍信息成功！")

    # 插入队员信息
    insert_athlete = """insert into athlete(athlete_number,athlete_name,athlete_sex,athlete_age,athlete_term) values
                (101,'石宇奇','男',28,'中国国家队'), \
                (102,'周天成','男',33,'中国国家队'),\
                (103,'李卓耀','男',26,'中国国家队'),\
                (104,'王子维','男',29,'中国国家队'), \
                (105,'陆光祖','男',27,'中国国家队'),\
                (106,'韩悦','女',25,'中国国家队'), \
                (107,'吴霞','女',27,'中国国家队'), \
                (201,'阿德·雷斯基·德维卡约','男',32,'阿塞拜疆国家队'), \
                (202,'赛·普拉尼斯','男',27,'印度尼西亚国家队'), \
                (203,'阿比纳夫·马诺塔','男',31,'新西兰国家队'),\
                (204,'阮进明','男',28,'越南国家队'), \
                (205,'托比·潘迪','男',28,'英格兰国家队'),\
                (206,'齐雪霏','女',28,'法国国家队'),\
                (207,'艾菲尔','女',28,'法国国家队')"""
    cursor.execute(insert_athlete)
    conn.commit()
    print("插入队员信息成功！")

    # 插入比赛信息
    insert_competition = """insert into competition(competition_time,competition_area,competition_contestant,competition_type,competition_result) values\
                ('2021-08-22 09:00:00','东京体育馆','石宇奇VS阿德-雷斯基-德维卡约','男单','2-0'),\
                ('2021-08-22 11:00:00','东京体育馆','周天成 VS 赛·普拉尼斯','男单','2-1'),\
                ('2021-08-22 14:00:00','东京体育馆','李卓耀 VS 阿比纳夫·马诺塔','男单','2-0'), \
                ('2021-08-22 16:00:00','东京体育馆','王子维 VS 阮进明','男单','2-0'), \
                ('2021-08-22 19:00:00','东京体育馆','陆光祖 VS 托比·潘迪','男单','2-0'), \
                ('2021-08-22 21:00:00','东京体育馆','韩悦 VS 齐雪霏','女单','2-0')"""
    cursor.execute(insert_competition)
    conn.commit()
    print("插入比赛信息成功！")

    # 插入参赛队员信息
    insert_competitors = """insert into competitors(competitors_number,competitors_name,competitors_time) values
                (01,'石宇奇 VS 阿德-雷斯基-德维卡约','2021-08-22 09:00:00'), \
                (02,'周天成 VS 赛·普拉尼斯','2021-08-22 11:00:00'), \
                (03,'李卓耀 VS 阿比纳夫·马诺塔','2021-08-22 14:00:00'), \
                (04,'王子维 VS 阮进明','2021-08-22 16:00:00'), \
                (05,'陆光祖 VS 托比·潘迪','2021-08-22 19:00:00'), \
                (06,'韩悦 VS 齐雪霏','2021-08-22 21:00:00')"""
    cursor.execute(insert_competitors)
    conn.commit()
    print("插入参赛队员信息成功！")




##-----------------------------------------------查询数据库数据-------------------------------------------##


def enquire_data():
    # 查询队伍信息
    enquire_term = 'select * from term;'
    result_term = pd.read_sql(enquire_term, conn)
    print("队伍信息如下：\n", result_term)

    # 查询队员信息
    enquire_athlete = 'select * from athlete;'
    result_athlete = pd.read_sql(enquire_athlete, conn)
    print("队员信息如下：\n", result_athlete)

    # 查询比赛信息
    enquire_competition = 'select * from competition;'
    result_competition = pd.read_sql(enquire_competition, conn)
    print("比赛信息如下：\n", result_competition)

    # 查询参赛队员信息
    enquire_competitors = 'select * from competitors;'
    result_competitors = pd.read_sql(enquire_competitors, conn)
    print("参赛队员信息如下：\n", result_competitors)



##---------------------------------------------------基本操作----------------------------------------##

#修改表名操作
def rename_tables(rename):
    old_tables = "show tables"
    old_df=pd.read_sql(old_tables,conn)
    print("原始表名：",old_df)
    cursor.execute(rename)
    new_tables='show tables'
    print("新表名：\n")
    new_df=pd.read_sql(new_tables,conn)
    print(new_df)


#添加新信息
def add_data(data):
    new_add = data
    cursor.execute(new_add)
    conn.commit()
    print("添加新信息成功！")


#查询数据
def real_data(data):
    # 查询参赛队员信息
    enquire_competitors = data
    result_competitors = pd.read_sql(enquire_competitors, conn)
    print("查询结果如下：\n", result_competitors)


#删除数据
def delete_data(sql):
    result = cursor.execute(sql)
    conn.commit()
    print("删除成功！\n")
    new_tables = 'select * from athlete'
    new_df = pd.read_sql(new_tables, conn)
    print(new_df)

#更新数据
def update(sql):
    cursor.execute(sql)
    conn.commit()
    print("更新数据成功！\n")




##---------------------------------------------------十六章前的操作----------------------------------------##

#查看表结构
sql_struct='describe team'
df_struct=pd.read_sql(sql_struct,conn)
print(df_struct)


#浮点数类型
sql_float1='create table if not exists float(a1 decimal,a2 decimal(5,2))'
cursor.execute(sql_float1)
print("表格创建成功")
sql_float2="""insert into float (d1,d2) values(3.13,3.141)"""
cursor.execute(sql_float2)
conn.commit()
print("数据插入成功！")



#聚合函数
sql_cluster='select count(*),max(athlete_age),min(athlete_age),avg(athlete_age) from athlete '
cursor.execute(sql_float2)
conn.commit()
df_struct=pd.read_sql(sql_cluster,conn)
print(df_struct)


#简单条件函数
enquire_competitors = 'select * from team where team_number between 3 and 6;'
result_competitors = pd.read_sql(enquire_competitors, conn)
print("参赛队员信息如下：\n", result_competitors)

enquire_competitors = 'select * from team where exists(select 4*3) and team_number=6;'
result_competitors = pd.read_sql(enquire_competitors, conn)
print("队伍信息如下：\n", result_competitors)


#创建视图
sql_view='create view view_athlete_number as select athlete_number from athlete'
cursor.execute(sql_view)
conn.commit()
print("创建视图成工！")
#查看视图
enquire_competitors = 'select * from view_athlete_number;'
result_competitors = pd.read_sql(enquire_competitors, conn)
print("视图信息如下：\n", result_competitors)


#存储过程与函数
#存储过程
sql_scro='create procedure selectAlldata() BEGIN select * from athlete;end;'
cursor.execute(sql_scro)
# conn.commit()
print("创建存储过程成功！")

#存储函数
sql_func='create function  select_namebyid() returns varchar(30) deterministic return ( select athlete_name from athlete where athlete_number=3)'
cursor.execute(sql_func)
# conn.commit()
print("创建存储函数成功！")

enquire_competitors = 'call selectalldata()'
result_competitors = pd.read_sql(enquire_competitors, conn)
print("存储过程信息如下：\n", result_competitors)
enquire_competitors2 = 'select select_namebyid()'
result_competitors2 = pd.read_sql(enquire_competitors2, conn)
print("存储函数信息如下：\n", result_competitors2)


#触发器
trigger='''create trigger before_insert before insert on athlete for each row
insert into athlete_number(athlete_name) values ('before_insert');'''
cursor.execute(trigger)
# conn.commit()
print("创建触发器成功！")

sql = 'show create trigger before_insert'
result_sql = pd.read_sql(sql, conn)
print("触发器信息如下：\n", result_sql)


#账户管理
#创建账户
new_user1='''CREATE user 'user1 '@'localhost';'''
cursor.execute(new_user1)
print("新用户创建成功！")
#删除用户
new_user1='''drop user 'user1 '@'localhost';'''
cursor.execute(new_user1)
print("新用户删除成功！")












if __name__ == '__main__':

    #球队信息录入
    create_tables()
    #insert_data()
    enquire_data()
    rename_tables(rename="""alter table term rename to team""")

    #增、删、改、查基本操作
    add_data(data="""insert into team(team_number,term_name,term_area) values(009,'美国国家队','美国')""")
    real_data('select * from team;')
    delete_data(sql= "select from athlete where athlete_age<26")
    update(sql="""update competition set time='2021-8-23 14:00:00' where competition_result='2-1'""")





