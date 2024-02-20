from XML_parser import initializer, sql_engine, Session

Base = initializer()
class ToDictMixin(object):
    def to_dict(self):
        return {c.key: getattr(self, c.key) for c in self.__table__.columns}

for class_ in Base.classes:
    class_.__bases__ = (ToDictMixin,) + class_.__bases__

def query_to_dict(session, table_name):
    try:
        table = getattr(Base.classes, table_name)
        query = session.query(table)
        result = query.first()
        if result:
            return result.to_dict()
    finally:
        session.close()

if __name__ == '__main__':
    session = Session()
    result_dict = query_to_dict(session, 'FeatureCodes')
    print(result_dict)
