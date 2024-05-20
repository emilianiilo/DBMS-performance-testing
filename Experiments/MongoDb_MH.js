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
});

//1_2
db.getCollection('person').aggregate(
  [
    { $unwind: '$employee.employment' },
    {
      $match: {
        'employee.employment.start_time': {
          $gte: ISODate(
            '2020-01-01T00:00:00.000Z'
          ),
          $lte: ISODate(
            '2022-12-31T23:59:59.000Z'
          )
        }
      }
    },
    {
      $project: {
        _id: 1,
        occupation_name:
          '$employee.employment.occupation_code',
        start_year: {
          $year: '$employee.employment.start_time'
        },
        end_year: {
          $year: '$employee.employment.end_time'
        }
      }
    },
    { $sort: { start_year: 1, _id: 1 } }
  ]);

//2_1
db.person.find({
    e_mail: 'acscu_ed69216fed359e36bd52421c86d40902@example.com'
}, {
    _id: 1,
    surname: 1
});

//2_2
db.getCollection('person').aggregate(
  [
    {
      $match: {
        e_mail:
          'bbsyi_691b8036185a3cef186bfadf34d5f14b@example.com'
      }
    },
    { $unwind: '$employee.employment' },
    {
      $match: {
        'employee.employment.occupation_code': 4,
        'employee.employment.start_time': ISODate(
          '2023-05-06T14:33:56.363Z'
        )
      }
    },
    {
      $project: {
        _id: 0,
        end_time: '$employee.employment.end_time'
      }
    }
  ]);

//3_1
db.getCollection('person').aggregate(
  [
    {
      $group: {
        _id: '$country_code',
        person_count: { $sum: 1 },
        avg_age_days: {
          $avg: {
            $divide: [
              {
                $subtract: [
                  '$reg_time',
                  '$birth_date'
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
        from: 'country',
        localField: '_id',
        foreignField: 'country_code',
        as: 'country_info'
      }
    },
    {
      $project: {
        _id: 0,
        country_code: '$_id',
        country_name: {
          $arrayElemAt: ['$country_info.name', 0]
        },
        person_count: 1,
        avg_age_days: 1
      }
    },
    { $sort: { country_code: 1 } }
  ]);

//3_2
db.getCollection('person').aggregate(
  [
    { $unwind: '$employee.employment' },
    {
      $lookup: {
        from: 'occupation',
        localField:
          'employee.employment.occupation_code',
        foreignField: 'occupation_code',
        as: 'occupation'
      }
    },
    { $unwind: '$occupation' },
    {
      $group: {
        _id: '$employee.employment.occupation_code',
        occupation_name: {
          $first: '$occupation.name'
        },
        employment_count: { $sum: 1 },
        avg_duration_days: {
          $avg: {
            $subtract: [
              '$employee.employment.end_time',
              '$employee.employment.start_time'
            ]
          }
        }
      }
    },
    {
      $project: {
        _id: 0,
        occupation_code: '$_id',
        occupation_name: 1,
        employment_count: 1,
        avg_duration_days: {
          $divide: [
            '$avg_duration_days',
            86400000
          ]
        }
      }
    }
  ]);

//3_3
db.getCollection('person').aggregate(
  [
    {
      $project: {
        _id: 1,
        e_mail: 1,
        employment_count: {
          $cond: {
            if: {
              $isArray: '$employee.employment'
            },
            then: {
              $size: '$employee.employment'
            },
            else: '0'
          }
        }
      }
    }
  ]);

//4_1
db.getCollection('person').aggregate(
  [
    { $match: { employee: { $ne: null } } },
    {
      $lookup: {
        from: 'employee_status_type',
        localField:
          'employee.employee_status_type_code',
        foreignField: 'employee_status_type_code',
        as: 'employee_status_info'
      }
    },
    {
      $project: {
        _id: 0,
        e_mail: 1,
        surname: 1,
        employee_status_type_name: {
          $arrayElemAt: [
            '$employee_status_info.name',
            0
          ]
        },
        employments: '$employee.employment'
      }
    }
  ]);

//4_2

db.getCollection('person').aggregate(
  [
    { $unwind: '$employee.employment' },
    {
      $lookup: {
        from: 'occupation',
        localField:
          'employee.employment.occupation_code',
        foreignField: 'occupation_code',
        as: 'occupation_info'
      }
    },
    {
      $project: {
        _id: 0,
        start_time:
          '$employee.employment.start_time',
        end_time: '$employee.employment.end_time',
        occupation_name: {
          $arrayElemAt: [
            '$occupation_info.name',
            0
          ]
        },
        e_mail: 1,
        surname: 1
      }
    }
  ]);

//5_1
db.getCollection('person').aggregate(
  [
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
  ]);

//6_1
db.person.insertOne({
  _id: 5,
  country_code: "USA",
  person_status_type_code: 3,
  nat_id_code: "1234567",
  e_mail: "example@example.com",
  birth_date: new Date("1931-06-03"),
  given_name: "eesnimi",
  surname: "perekonnanimi",
  address: "Random 123",
  tel_nr: "+1 553344",
  reg_time: new Date("2024-04-09")
});

db.employee.insertOne({
  person_id: 5,
  mentor_id: 4673567,
  employee_status_type_code: 2
});

//6_2
db.employment.insert({
  person_id: 5,
  occupation_code: 1,
  start_time: new Date("2024-01-01")
});

//7_1
db.person.updateOne(
  { e_mail: "example@example.com" },
  { $set: { tel_nr: "+1 566666" } }
)

//7_2
db.employment.updateOne(
  {
    "person_id": {
      $eq: db.person.findOne({ "e_mail": "example@example.com" })._id
    },
    "occupation_code": 1,
    "start_time": new Date("2024-01-01")
  },
  {
    $set: {
      "end_time": new Date("2024-04-20")
    }
  }
);

//8_1
db.person.updateMany(
  { "_id": { $in: db.employment.distinct("person_id", { "occupation_code": { $gte: 10, $lte: 30 } }) } },
  { $unset: { "address": "" } }
);

//8_2
db.employment.updateMany(
  {
    "person_id": {
      $in: db.person.find({
        "person_status_type_code": { $in: [1, 2] },
        "country_code": "EST"
      }, { _id: 1 }).toArray().map(person => person._id)
    }
  },
  { $unset: { "end_time": "" } }
);

//9_1
db.person.deleteOne({ e_mail: "example@example.com" });

//9_2
db.employment.deleteOne({
  "person_id": {
    $eq: db.person.findOne({ "e_mail": "bbsyi_691b8036185a3cef186bfadf34d5f14b@example.com" })._id
  },
  "occupation_code": 27,
  "start_time": new Date("2022-03-11T20:32:51.062824")
});

//10_1
db.person.deleteMany({ "_id": { $in: db.employment.distinct("person_id", { "occupation_code": { $gte: 10, $lte: 30 } }) } });

//10_2
db.employment.deleteMany({
  "person_id": {
    $in: db.person.find({
      "person_status_type_code": { $in: [1, 2] },
      "country_code": "EST"
    }, { _id: 1 }).toArray().map(person => person._id)
  }
});





