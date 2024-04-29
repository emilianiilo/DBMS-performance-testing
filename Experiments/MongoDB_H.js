//1_1
db.person.find({
    birth_date: {
        $gte: ISODate("1940-01-01T00:00:00Z"),
        $lte: ISODate("1980-12-31T23:59:59Z")
    }
}, {
    _id: 1,
    e_mail: 1,
    birth_date: 1
}).sort({
    birth_date: 1,
    e_mail: 1
})

//1_2

db.person.aggregate([
    {
        $unwind: "$employee.employment"
    },
    {
        $match: {
            "employee.employment.start_time": {
                $gte: ISODate("2020-01-01T00:00:00Z"),
                $lte: ISODate("2022-12-31T23:59:59Z")
            }
        }
    },
    {
        $project: {
            _id: 1,
            occupation_name: "$employee.employment.occupation_code",
            start_year: {
                $year: "$employee.employment.start_time"
            },
            end_year: {
                $year: "$employee.employment.end_time"
            }
        }
    },
    {
        $sort: {
            start_year: 1,
            _id: 1
        }
    }
])

//2_1

db.person.find({
    e_mail: 'acscu_ed69216fed359e36bd52421c86d40902@example.com'
}, {
    _id: 1,
    surname: 1
})

//2_2

db.person.aggregate([
    {
        $match: {
            e_mail: 'jnlpu_b07559ff04737ce21a10bb8a5438ac62@example.com'
        }
    },
    {
        $unwind: "$employee.employment"
    },
    {
        $match: {
            "employee.employment.occupation_code": 42,
            "employee.employment.start_time": ISODate("2023-03-11T20:32:51.062Z")
        }
    },
    {
        $project: {
            end_time: "$employee.employment.end_time"
        }
    }
])

//3_1

db.person.aggregate([
  {
        $group: {
            _id: "$country_code",
            person_count: {
                $sum: 1
            },
            avg_age_days: {
                $avg: {
                    $divide: [
                        {
                            $subtract: [
                                "$reg_time",
                                "$birth_date"
                            ]
                        },
                        86400000
                    ]
                }
            }
        }
    },
    {
        $lookup: {
          
            from: "country",
            localField: "_id",
            foreignField: "country_code",
            as: "country_info"
        }
    },
    {
        $project: {
          _id: 0,
            country_code: "$_id",
            country_name: {
                $arrayElemAt: [
                    "$country_info.name",
                    0
                ]
            },
            person_count: 1,
            avg_age_days: 1
        }
    },
    {
        $sort: {
            country_code: 1
        }
    }
])

// 3_2

db.person.aggregate([
  {
    $unwind: "$employee.employment"
  },
  {
    $lookup: {
      from: "occupation",
      localField: "employee.employment.occupation_code",
      foreignField: "occupation_code",
      as: "occupation"
    }
  },
  {
    $unwind: "$occupation"
  },
  {
    $group: {
      _id: "$employee.employment.occupation_code",
      occupation_name: { $first: "$occupation.name" },
      employment_count: { $sum: 1 },
      avg_duration_days: { $avg: { $subtract: ["$employee.employment.end_time", "$employee.employment.start_time"] } }
    }
  },
  {
    $project: {
      _id: 0,
      occupation_code: "$_id",
      occupation_name: 1,
      employment_count: 1,
      avg_duration_days: { $divide: ["$avg_duration_days", 86400000] }
    }
  }
]
)


//3_3
db.person.aggregate([
    {
        $project: {
            _id: 1,
            e_mail: 1,
            employment_count: {
                $cond: {
                    if: {
                        $isArray: "$employee.employment"
                    },
                    then: {
                        $size: "$employee.employment"
                    },
                    else: "0"
                }
            }
        }
    }
])


//4_1
db.person.aggregate([
  {
    $match: {
      employee: {
        $ne: null,
      },
    },
  },
  {
    $lookup: {
      from: "employee_status_type",
      localField:
        "employee.employee_status_type_code",
      foreignField: "employee_status_type_code",
      as: "employee_status_info",
    },
  },
  {
    $project: {
      _id: 0,
      e_mail: 1,
      surname: 1,
      employee_status_type_name: {
        $arrayElemAt: [
          "$employee_status_info.name",
          0,
        ],
      },
      employments: "$employee.employment",
    },
  },
])


//4_2
db.person.aggregate([
  {
    $unwind: "$employee.employment",
  },
  {
    $lookup: {
      from: "occupation",
      localField:
        "employee.employment.occupation_code",
      foreignField: "occupation_code",
      as: "occupation_info",
    },
  },
  {
    $project: {
      _id: 0,
      start_time:
        "$employee.employment.start_time",
      end_time: "$employee.employment.end_time",
      occupation_name: {
        $arrayElemAt: [
          "$occupation_info.name",
          0,
        ],
      },
      e_mail: 1,
      surname: 1,
    },
  },
])

// 5_1

db.person.aggregate([
    {
      $lookup: {
        from: 'employee_status_type',
        localField:
          'employee.employee_status_type_code',
        foreignField: 'employee_status_type_code',
        as: 'employee_status'
      }
    },
    {
      $lookup: {
        from: 'person',
        localField: 'employee.mentor_id',
        foreignField: '_id',
        as: 'mentor'
      }
    },
    { $unwind: '$mentor' },
    {
      $lookup: {
        from: 'employee_status_type',
        localField:
          'mentor.employee.employee_status_type_code',
        foreignField: 'employee_status_type_code',
        as: 'mentor_status'
      }
    },
    {
      $match: {
        $expr: {
          $ne: [
            '$employee.employee_status_type_code',
            '$mentor.employee.employee_status_type_code'
          ]
        }
      }
    },
    {
      $project: {
        _id: 0,
        employee_email: '$e_mail',
        employee_status: {
          $arrayElemAt: [
            '$employee_status.name',
            0
          ]
        },
        mentor_email: '$mentor.e_mail',
        mentor_status: {
          $arrayElemAt: ['$mentor_status.name', 0]
        }
      }
    }
  ])



//6_1

db.person.insertOne({
  	_id: 5,
    nat_id_code: "1234567",
    country_code: "USA",
    person_status_type_code: 3,
    e_mail: "example@example.com",
    birth_date: new Date("1931-06-03"),
    given_name: "eesnimi",
    surname: "perekonnanimi",
    address: "Random 123",
    tel_nr: "+1 553344",
    reg_time:new Date("2024-04-09"),
    employee: {
        employee_status_type_code: 2,
        mentor_id: 4673567,
        employment: []
    }
})

//6_2
db.person.updateOne(
    { given_name: "eesnimi" }, 
    {
        $push: {
            "employee.employment": {
                occupation_code: 1,
                start_time: new Date("2024-01-01")
            }
        }
    }
)

//7_1

db.person.updateOne(
    { e_mail: "example@example.com" },
    { $set: { tel_nr: "+1 566666" } }
)

//7_2
db.person.updateOne(
    {
        e_mail: "example@example.com",
        "employee.employment": {
            $elemMatch: {
                occupation_code: 1,
                start_time: new Date("2024-01-01")
            }
        }
    },
    { $set: { "employee.employment.$.end_time": new Date("2024-03-20") } }
)

//8_1 vol1
db.person.updateMany(
    { "employee.employment.occupation_code": { $gte: 10, $lte: 30 } },
    { $set: { address: null } }
)

//8_1 vol2
db.person.updateMany(
    { "employee.employment.occupation_code": { $gte: 10, $lte: 30 } },
    { $set: { address: "Random 123" } }
)

//8_2
db.person.updateMany(
    {
        country_code: "EST",
        "employee.employee_status_type_code": { $in: [1, 2] }
    },
    { $set: { "employee.employment.$[].end_time": null } }
)

//9_1
db.person.deleteOne({ e_mail: "example@example.com" })

//9_2
db.person.updateOne(
    {"e_mail": "bbsyi_691b8036185a3cef186bfadf34d5f14b@example.com"},
    {
        "$pull": {
            "employee.employment": {
                "occupation_code": 27,
                "start_time": ISODate("2022-03-11T20:32:51.062Z")
            }
        }
    }
)

//10_1
db.person.deleteMany({ "employee.employment.occupation_code": { $gte: 10, $lte: 30 } })



//10_2
db.person.updateMany(
    {
        country_code: "EST",
        "employee.employee_status_type_code": { $in: [1, 2] }
    },
    { $set: { "employee.employment": [] } }
)


