
from flask_sqlalchemy import SQLAlchemy

db = SQLAlchemy()
class Coin(db.Model):
  id = db.Column(db.String(80), primary_key=True)
  exchanges = db.Column(db.Text, unique=False, primary_key=False)
  task_run = db.Column(db.BigInteger, unique=False, primary_key=False)

  def __init__(self, id, exchanges, task_run):
    self.id = id
    self.exchanges = exchanges
    self.task_run = task_run
