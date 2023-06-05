from ....extensions import db
from sqlalchemy.orm import synonym
from sqlalchemy import and_, or_, not_


class TaskLog(db.Model):
    """Node Model represent table, `ctr_task_process`"""
    __tablename__ = 'ctr_task_process'
    __table_args__ = {
        # 'autoload': True,
        'extend_existing': True,
        'autoload_with': db.engine
    }

    id = db.Column('process_id', db.String(64), primary_key=True, nullable=False)
    type = db.Column('process_type', db.String(64), nullable=False)
    name_put = db.Column('process_name_put', db.String(256), nullable=False)
    name_get = db.Column('process_name_get', db.String(256), nullable=True)
    run_date_put = db.Column('run_date_put', db.String(256), nullable=False)
    run_date_get = db.Column('run_date_get', db.Date, nullable=True)
    update_date = db.Column('update_date', db.DateTime(timezone=True), nullable=False)
    message = db.Column('process_message', db.Text, nullable=True)
    process_time = db.Column('process_time', db.String(128), nullable=False)
    number_put = db.Column('process_number_put', db.String(32), nullable=True)
    number_get = db.Column('process_number_get', db.String(32), nullable=True)
    module = db.Column('process_module', db.String(64), nullable=False)
    status = db.Column('status', db.String(32), nullable=False)

    # Setup the Synonym attribute.
    name = synonym('name_put')
    run_date = synonym('run_date_put')

    @property
    def alert(self):
        mapping = {
            0: 'success',
            1: 'danger',
            2: 'warning'
        }
        return mapping.get(int(self.status), 2)

    def to_dict(self):
        return {
            'id': self.id,
            'type': self.type,
            'module': self.module,
            'name': self.name_put,
            'run_date': self.run_date_put.strip('[]').replace("'", ""),
            'status': self.status,
            'process_time': self.process_time,
            'message': self.message,
        }

    @classmethod
    def search_all(cls, query: str):
        _query: str = query.replace('*', '%')
        return cls.query.filter(
            or_(*[
                col.ilike(f'%{_query}%')
                for col in [
                    cls.id,
                    cls.type,
                    cls.name,
                    cls.run_date
                ]
            ])
        )

    @classmethod
    def search_by(cls, query: dict):
        return cls.query.filter(
            and_(*[
                cls.get_column(col).ilike(f'%{value.replace("*", "%")}%')
                for col, value in query.items()
            ])
        )
