# Name: Ava Mo

import pymongo

client = pymongo.MongoClient()

db = client["college"]

col = db["faculty"]

result_info = col.delete_many({})

doclist=[
    {
        "name":"Zhang",
        "dept_name":"CS",
        "salary":68000,
        "teach":[
            {
                "course_id":"CS-101",
                "year":2023
            },
            {
                "course_id":"CS-347",
                "year":2023
            }
        ]
    },
    {
        "name":"Levy",
        "dept_name":"CS",
        "salary":80000,
        "teach":[
            {
                "course_id":"CS-128",
                "year":2021
            },
            {
                "course_id":"CS-201",
                "year":2022
            },
            {
                "course_id":"CS-201",
                "year":2023
            }
        ]
    },
    {
        "name":"Brandt",
        "dept_name":"Finance",
        "salary":95000,
        "teach":[
            {
                "course_id":"FIN-201",
                "year":2021
            },
            {
                "course_id":"FIN-301",
                "year":2022
            },
            {
                "course_id":"FIN-320",
                "year":2022
            },
            {
                "course_id":"FIN-320",
                "year":2023
            }
        ]
    },
    {
        "name":"Davis",
        "dept_name":"Finance",
        "salary":78000,
        "teach":[
            {
                "course_id":"FIN-102",
                "year":2022
            },
            {
                "course_id":"FIN-200",
                "year":2023
            }
        ]
    },
    {
        "name":"Crick",
        "dept_name":"Biology",
        "salary":92000,
        "teach":[
            {
                "course_id":"BIO-101",
                "year":2022
            },
            {
                "course_id":"BIO-301",
                "year":2022
            },
            {
                "course_id":"BIO-301",
                "year":2023
            }
        ]
    },
    {
        "name":"Williams",
        "dept_name":"Biology",
        "salary":72000,
        "teach":[
            {
                "course_id":"BIO-101",
                "year":2021
            },
            {
                "course_id":"BIO-201",
                "year":2022
            },
            {
                "course_id":"BIO-101",
                "year":2023
            },
            {
                "course_id":"BIO-201",
                "year":2023
            }
        ]
    }
]

col.insert_many(doclist)

pipeline = [
    { "$unwind": "$teach" },
    { "$group": {
        "_id": { "dept_name": "$dept_name", "year": "$teach.year" },
        "num_instructors": { "$addToSet": "$name" }
    }},
    { "$addFields": {
        "num_instructors": { "$size": "$num_instructors" }
    }},
    { "$sort": { "_id.dept_name": 1, "_id.year": -1 }},
    { "$project": {
        "dept_name": "$_id.dept_name",
        "year": "$_id.year",
        "num_instructors": "$num_instructors",
        "_id": 0
    }}
]

result = col.aggregate(pipeline)

for doc in result:
    print(doc)

client.close()
