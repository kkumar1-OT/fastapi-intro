from datetime import datetime
from typing import Optional

from pydantic import BaseModel, Field

class TagBase(BaseModel):
    name: str = Field(regex="^[a-z_]{3,15}$")
    value: int= Field(lt=10)

class TagId(TagBase):
    id: str = None

class Tag(TagBase):
    id: str = None
    created_at: Optional[datetime] = Field(default_factory=datetime.utcnow)
    updated_at: datetime = Field(default_factory=datetime.utcnow)

    class Config:
        orm_mode = True

class TagUpdate(TagBase):
    updated_at: datetime = Field(default_factory=datetime.utcnow)