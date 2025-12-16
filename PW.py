from peewee import *


db = SqliteDatabase('data.db')


class BaseModel(Model):
    class Meta:
        database = db


class Students(BaseModel):
    s_id = IntegerField(primary_key=True)
    first_name = CharField()
    last_name = CharField()
    age = IntegerField()
    city = CharField()

    class Meta:
        table_name = 'Students'


class Courses(BaseModel):
    c_id = IntegerField(primary_key=True)
    c_name = CharField()
    time_start = CharField()
    time_end = CharField()

    class Meta:
        table_name = 'Courses'


class StudentCourses(BaseModel):
    course_id = IntegerField()
    student_id = IntegerField()

    class Meta:
        table_name = 'StudentCourses'
        primary_key = False
        indexes = (
            (('course_id', 'student_id'), True),
        )


class SQL():
    @staticmethod
    def sql_connection():
        try:
            db.connect()
            print("\n***connection successful***\n")
            return db
        except OperationalError as error:
            print("\n***connection unsuccessful***\n", error)
            return None

    @staticmethod
    def create_table_students(con):
        try:
            db.create_tables([Students], safe=True)
        except OperationalError as error:
            print(error)

    @staticmethod
    def create_table_courses(con):
        try:
            db.create_tables([Courses], safe=True)
        except OperationalError as error:
            print(error)

    @staticmethod
    def create_table_student_courses(con):
        try:
            db.create_tables([StudentCourses], safe=True)
        except OperationalError as error:
            print(error)

    @staticmethod
    def students_add_data(con, values: tuple):
        try:
            Students.insert({
                's_id': values[0],
                'first_name': values[1],
                'last_name': values[2],
                'age': values[3],
                'city': values[4]
            }).on_conflict_ignore().execute()
        except Exception as error:
            print(error)

    @staticmethod
    def courses_add_data(con, values: tuple):
        try:
            Courses.insert({
                'c_id': values[0],
                'c_name': values[1],
                'time_start': values[2],
                'time_end': values[3]
            }).on_conflict_ignore().execute()
        except Exception as error:
            print(error)

    @staticmethod
    def student_courses_add_data(con, values: tuple):
        try:
            StudentCourses.insert({
                'course_id': values[0],
                'student_id': values[1]
            }).on_conflict_ignore().execute()
        except Exception as error:
            print(error)

    @staticmethod
    def get_student_older_than(con, min_age: int):
        try:
            query = (Students
                     .select(Students.first_name, Students.last_name)
                     .where(Students.age >= min_age))

            print(f'Студенты, старше {min_age} лет:')
            for student in query:
                print(f'{student.last_name}\t{student.first_name}')
            print('\n')
        except Exception as error:
            print(error)

    @staticmethod
    def get_student_studying(con, course_name: str):
        try:
            query = (Students
                     .select(Students.first_name, Students.last_name)
                     .join(StudentCourses, on=(Students.s_id == StudentCourses.student_id))
                     .join(Courses, on=(StudentCourses.course_id == Courses.c_id))
                     .where(Courses.c_name == course_name))

            print(f'Студенты, обучающиеся на курсе {course_name}:')
            for student in query:
                print(f'{student.last_name}\t{student.first_name}\n')
        except Exception as error:
            print(error)

    @staticmethod
    def get_student_studying_from_city(con, city_name: str, course_name: str):
        try:
            query = (Students
                     .select(Students.first_name, Students.last_name)
                     .join(StudentCourses, on=(Students.s_id == StudentCourses.student_id))
                     .join(Courses, on=(StudentCourses.course_id == Courses.c_id))
                     .where((Students.city == city_name) & (Courses.c_name == course_name)))

            print(f'Студенты, обучающиеся на курсе {course_name} и проживающие в {city_name}:')
            for student in query:
                print(f'{student.last_name}\t{student.first_name}\n')
        except Exception as error:
            print(error)

    @staticmethod
    def any_command(con, command: str):
        try:
            if 'SELECT' in command.upper():
                cursor = db.execute_sql(command)
                for row in cursor.fetchall():
                    print(row)
            else:
                db.execute_sql(command)
        except Exception as error:
            print(error)

    @staticmethod
    def finish_session(con):
        try:
            db.close()
            print("\n***disconnected from database***\n")
        except Exception as error:
            print(error)