import time
import random
from datetime import datetime, timezone
import pyodbc
from pymongo import MongoClient

sql_conn = pyodbc.connect(
    "Driver={ODBC Driver 17 for SQL Server};"
    "Server=DESKTOP-VM46I0R;"
    "Database=DB_Lab1;"
    "Trusted_Connection=yes;"
)
sql_cursor = sql_conn.cursor()

mongo_client = MongoClient("mongodb://localhost:27017/")
mongo_db = mongo_client["education_platform"]
mongo_courses = mongo_db["courses"]

def generate_data(num_courses=1000):
    courses = []
    for i in range(1, num_courses + 1):
        modules = []
        for j in range(1, random.randint(2, 6)):
            lessons = [
                {
                    "id": k,
                    "title": f"Lesson {k} in Module {j} of Course {i}",
                    "content": f"Content of lesson {k}",
                    "creation_date": datetime.now(timezone.utc),
                }
                for k in range(1, random.randint(3, 8))
            ]
            modules.append({
                "id": j,
                "title": f"Module {j} of Course {i}",
                "description": f"Description of Module {j}",
                "lessons": lessons,
            })
        courses.append({
            "title": f"Course {i}",
            "description": f"Description of Course {i}",
            "creation_date": datetime.now(timezone.utc),
            "modules": modules,
        })
    return courses

def insert_data_mongo(courses):
    start_time = time.time()
    mongo_courses.insert_many(courses)
    print(f"MongoDB Insert Time: {time.time() - start_time:.2f} seconds")

def insert_data_sql(courses):
    start_time = time.time()
    for course in courses:
        try:
            sql_cursor.execute(
                """
                INSERT INTO Courses (Title, Description, Creation_date, Updated_by, Is_deleted, Category_id)
                OUTPUT inserted.Id
                VALUES (?, ?, GETDATE(), 1, 0, 1);
                """,
                (course["title"], course["description"])
            )
            result = sql_cursor.fetchone()
            sql_conn.commit()

            if result:
                course_id = result[0]
            else:
                print(f"Не вдалося отримати Id для курсу: {course['title']}")
                continue

            for module in course["modules"]:
                sql_cursor.execute(
                    """
                    INSERT INTO Modules (Title, Description, Creation_date, Updated_by, Is_deleted, Course_id)
                    OUTPUT inserted.Id
                    VALUES (?, ?, GETDATE(), 1, 0, ?);
                    """,
                    (module["title"], module["description"], course_id)
                )
                result = sql_cursor.fetchone()
                sql_conn.commit()

                if result:
                    module_id = result[0]
                else:
                    print(f"Не вдалося отримати Id для модуля: {module['title']}")
                    continue

                for lesson in module["lessons"]:
                    sql_cursor.execute(
                        """
                        INSERT INTO Lessons (Title, Content, Creation_date, Updated_by, Is_deleted, Module_id)
                        VALUES (?, ?, GETDATE(), 1, 0, ?);
                        """,
                        (lesson["title"], lesson["content"], module_id)
                    )
                    sql_conn.commit()
        except pyodbc.Error as e:
            print(f"Помилка під час вставки курсу '{course['title']}': {e}")
            sql_conn.rollback()

    print(f"SQL Insert Time: {time.time() - start_time:.2f} seconds\n")

def test_read_mongo():
    start_time = time.time()
    courses = mongo_courses.find({})
    total_courses = sum(1 for _ in courses)
    print(f"MongoDB Read Time: {time.time() - start_time:.2f} seconds for {total_courses} courses")

def test_read_sql():
    start_time = time.time()
    sql_cursor.execute("SELECT COUNT(*) FROM Courses")
    total_courses = sql_cursor.fetchone()[0]
    print(f"SQL Read Time: {time.time() - start_time:.2f} seconds for {total_courses} courses")

if __name__ == "__main__":
    courses_data = generate_data(1000)

    insert_data_mongo(courses_data)

    insert_data_sql(courses_data)

    test_read_mongo()

    test_read_sql()