from typing import List
from google.cloud import firestore
from firebase_admin import firestore, credentials
import firebase_admin
import os
from schemas.tag_schema import Tag, TagId

''' Firestore configuration (to connect from Local)'''
# cred = credentials.ApplicationDefault()
# os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "key.json"
# firebase_app = firebase_admin.initialize_app(cred)
# db = firestore.Client()

'''Below line is enough to connect from Firestore from Google cloud'''
db = firestore.Client(project='fastapi-intro')

class TagDao:
    collection_name = "tags"
    col_ref = db.collection(
            collection_name)

    def create(self, tag_create: Tag) -> Tag:
        data = tag_create.dict()
        data["id"] = str(data["id"])
        doc_ref = db.collection(
            self.collection_name).document(data["id"])
        doc_ref.set(data)
        return self.get(data["id"])
    
    def update(self, tag_update:Tag) -> Tag:
        data = tag_update.dict()
        tag_doc = self.col_ref.where("name", '==', data["name"]).get()
        print(tag_doc)
        if len(tag_doc)>0:
            self.col_ref.document(tag_doc[0].id).update({ 'value': tag_doc[0].to_dict()['value']+data["value"], 'updated_at': data["updated_at"] })
        else:
            self.col_ref.document().create(data)
        return "Tag count updated successfully"

    def get(self, id: TagId) -> Tag:
        doc_ref = self.col_ref.document(str(id))
        doc = doc_ref.get()
        if doc.exists:
            return Tag(**doc.to_dict())
        return

    def list(self) -> List[Tag]:
        doc_res=dict()
        tag_docs = self.col_ref.stream()
        if tag_docs:
            for doc in tag_docs:
                doc_dict = doc.to_dict()
                doc_res[doc_dict.get("name")] = doc_dict.get("value")
        return doc_res