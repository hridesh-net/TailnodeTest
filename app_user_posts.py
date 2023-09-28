import os
import httpx
import logging
import asyncio
from sqlalchemy import ForeignKey
from sqlalchemy.orm import sessionmaker, relationship
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import create_engine, Column, Integer, String


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


# Defining the sqlite database
database_url = "sqlite:///database/UserPostsDatabase.db"

engine = create_engine(database_url, echo=True)
Base = declarative_base()


# User data table
class UserTable(Base):
    __tablename__ = "usertable"

    id = Column(String, primary_key=True)
    title = Column(String)
    firstname = Column(Integer)
    lastname = Column(String)
    picture_url = Column(String)

    posts = relationship("PostsData", back_populates="owner")


# Posts data table
class PostsData(Base):
    __tablename__ = "posts_data"

    id = Column(String, primary_key=True)
    image = Column(String)
    likes = Column(Integer)
    text = Column(String)
    publishDate = Column(String)
    owner_id = Column(String, ForeignKey("usertable.id"))

    owner = relationship("UserTable", back_populates="posts")


Base.metadata.create_all(engine)

Session = sessionmaker(bind=engine)
session = Session()


async def get_users():
    headers = {"app-id": "65140c6cbac0bd91ff4cabc0"}

    page = 0
    limit = 20
    total_data = []
    async with httpx.AsyncClient() as client:
        try:
            while True:
                user_data_url = (
                    f"https://dummyapi.io/data/v1/user?page={page}&limit={limit}"
                )
                res = await client.get(user_data_url, headers=headers)
                res.raise_for_status()
                data = res.json()

                total_data.extend(data["data"])

                if page * limit < data["total"]:
                    page += 1
                else:
                    break

            return total_data if res.status_code == 200 else f"Error: {res.status_code}"

        except httpx.TimeoutException as te:
            logger.error("Request timed out")
        except httpx.HTTPStatusError as e:
            logger.error("HTTP error for user")
        except Exception as ex:
            logger.error(f"Error: {ex}")


async def fetch_user_data_and_posts(user):
    user_id = user.id
    headers = {"app-id": "65140c6cbac0bd91ff4cabc0"}

    page = 0
    limit = 20
    total_posts = []
    async with httpx.AsyncClient() as client:
        try:
            while True:
                posts_url = f"https://dummyapi.io/data/v1/user/{user_id}/post?page={page}&limit={limit}"
                res = await client.get(posts_url, headers=headers, timeout=10)
                res.raise_for_status()
                data = res.json()

                total_posts.extend(data["data"])

                if page * limit < data["total"]:
                    page += 1
                else:
                    break

            return total_posts

        except httpx.TimeoutException as te:
            logger.error(f"Request timed out for user {user_id}: {te}")
        except httpx.HTTPStatusError as e:
            logger.error(f"HTTP error for user {user_id}: {e}")
        except Exception as ex:
            logger.error(f"Error for user {user_id}: {ex}")


if __name__ == "__main__":
    logger.info(msg="Extracting user data...")
    users_data = asyncio.run(get_users())

    for data in users_data:
        data_to_insert = UserTable(
            id=data["id"],
            title=data["title"],
            firstname=data["firstName"],
            lastname=data["lastName"],
            picture_url=data["picture"],
        )
        session.add(data_to_insert)

    session.commit()
    
    logger.info(msg="User data saved :)")

    users = session.query(UserTable).all()
    
    logger.info(msg="Extracting user's posts...")
    for user in users:
        posts_data = asyncio.run(fetch_user_data_and_posts(user))

        for data in posts_data:
            post = PostsData(
                id=data["id"],
                image=data["image"],
                likes=data["likes"],
                text=data["text"],
                publishDate=data["publishDate"],
                owner_id=user.id,
            )
            session.add(post)

        session.commit()

    logger.info(msg="Extraction done check db")
    session.commit()
    session.close()
