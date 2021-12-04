from fastapi import FastAPI, Request, Response
import json
import traceback
from schemas.tag_schema import Tag
from services.tag_service import TagService

''' Initializing FastAPI '''
app = FastAPI()
tag_service = TagService()

'''sample API '''
@app.get("/")
def hello():
    return "Hello! Welcome to sample FastAPI app."

'''Health Check API'''
@app.get("/health")
def health():
    return {"status": "Ok"}

'''API for Incrementing Tag count'''
@app.post("/increment-tag-count", tags=["tags"])
def increment_tag_value(request: Tag):
    try:
        tag = tag_service.update_tag(request)
        return tag
    except Exception:
        print(traceback.format_exc())
        return (f"An Error Occured..!")

'''API to get list of Tag stats'''
@app.get("/tag-stats")
def get_tag_stats():
    tags_list = tag_service.list_tags()
    return Response(json.dumps(tags_list, sort_keys=True, default=str), media_type="application/json")    
