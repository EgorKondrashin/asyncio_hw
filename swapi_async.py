import asyncio
import datetime
import aiohttp
from more_itertools import chunked
from models import engine, Session, Base, SwapiPeople, get_model_keys


async def get_people(people_id):

    async with aiohttp.ClientSession() as session:
        response = await session.get(f'https://swapi.dev/api/people/{people_id}')
        json_data = await response.json()
        json_data['id'] = people_id
        # await session.close()
        return json_data


async def paste_to_db(person_json):
    async with Session() as session:
        for json in person_json:
            keys = await get_model_keys()
            filtered_dict = {key: value for key, value in json.items() if key in keys}
            person = SwapiPeople(**filtered_dict)
            session.add(person)
        await session.commit()


async def main():

    async with engine.begin() as con:
        await con.run_sync(Base.metadata.drop_all)
        await con.run_sync(Base.metadata.create_all)

    person_coros = (get_people(i) for i in range(1, 84))

    person_coros_chunked = chunked(iter(person_coros), 5)

    for person_coros_chunk in person_coros_chunked:
        persons = await asyncio.gather(*person_coros_chunk)
        asyncio.create_task(paste_to_db(persons))
    tasks = asyncio.all_tasks() - {asyncio.current_task(), }
    await asyncio.gather(*tasks)


if __name__ == '__main__':
    start = datetime.datetime.now()
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    asyncio.run(main())
    print(f'Загрузка заняла {datetime.datetime.now() - start} времени')
