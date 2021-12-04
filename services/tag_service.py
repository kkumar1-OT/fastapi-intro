from typing import List
from daos.tag_dao import TagDao
from schemas.tag_schema import Tag

tag_dao = TagDao()

class TagService:
    def create_tag(self, tag_create: Tag) -> Tag:
        return tag_dao.create(tag_create)

    def update_tag(self, tag_update: Tag) -> Tag:
        return tag_dao.update(tag_update)

    def get_tag(self, id: str) -> Tag:
        return tag_dao.get(id)
    
    def list_tags(self) -> List[Tag]:
        return tag_dao.list()