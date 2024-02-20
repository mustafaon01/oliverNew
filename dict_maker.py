from XML_parser import initializer, sql_engine, Session

Base = initializer()
class ToDictMixin(object):
    def to_dict(self):
        return {c.key: getattr(self, c.key) for c in self.__table__.columns}

for class_ in Base.classes:
    class_.__bases__ = (ToDictMixin,) + class_.__bases__

def query_to_dict(session, table_name, field_name, field_value):
    try:
        table = getattr(Base.classes, table_name)
        results = session.query(table).filter(getattr(table, field_name) == field_value).all()
        return [result.to_dict() for result in results]

    finally:
        session.close()

if __name__ == '__main__':
    session = Session()
    result_dict = query_to_dict(session, 'Lighting', 'LightingNames', "('0_configurator_cameras', '0_lighting')")
    print(result_dict)
