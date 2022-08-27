import os
from data_model import db, Coin
from flask import Flask, request, jsonify
from pipeline import ingest_data

app = Flask(__name__)
app.config['SQLALCHEMY_DATABASE_URI'] = os.environ.get('DATABASE_URL')
db.init_app(app)

if __name__ == '__main__':
    app.run(debug=True, threaded=True)

@app.before_first_request
def initialize_database():
    db.create_all()

@app.route('/coins', methods=['POST'])
def create_item():
  body = request.get_json()
  task_groups = ingest_data(body)
  return f"{task_groups} record(s) added"

# List all record of <id>
@app.route('/coins/<id>', methods=['GET'])
def get_item(id):
  item = Coin.query.get(id)
  del item.__dict__['_sa_instance_state']
  return jsonify(item.__dict__)

# List all records
@app.route('/coins', methods=['GET'])
def get_items():
  items = []
  for item in db.session.query(Coin).all():
    del item.__dict__['_sa_instance_state']
    items.append(item.__dict__)
  return jsonify(items)

# Update a record
@app.route('/coins/<id>', methods=['PUT'])
def update_item(id):
  body = request.get_json()
  db.session.query(Coin).filter_by(id=id).update(
    dict(title=body['title'], content=body['content']))
  db.session.commit()
  return "record updated"

# Delete a record
@app.route('/coins/<id>', methods=['DELETE'])
def delete_item(id):
  db.session.query(Coin).filter_by(id=id).delete()
  db.session.commit()
  return "record deleted"
