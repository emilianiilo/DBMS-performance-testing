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
db.employment.aggregate([{
    $match: {
      "start_time": {
        $gte: ISODate("2020-01-01"),
        $lte: ISODate("2022-12-31")
      },
      "end_time": { $exists: true }
    }
  },
  {
    $lookup: {
      from: "occupation",
      localField: "occupation_code",
      foreignField: "occupation_code",
      as: "occupation"
    }
  },
  {
    $project: {
      _id: 0,
      person_id: 1,
      "occupation.name": 1,
      start_year: { $year: "$start_time" },
      end_year: { $year: "$end_time" }
    }
  },
  {
    $sort: {
      start_year: 1,
      person_id: 1
    }
  }]);

//2_1
db.person.find({
    e_mail: 'acscu_ed69216fed359e36bd52421c86d40902@example.com'
}, {
    _id: 1,
    surname: 1
}).explain("executionStats");

//2_2
db.employment.aggregate([
  {
    $match: {
      occupation_code: 42,
      start_time: ISODate(
        "2023-03-11T18:32:51.062+00:00"
      ),
    },
  },
  {
    $lookup: {
      from: "person",
      localField: "person_id",
      foreignField: "_id",
      as: "person",
    },
  },
  {
    $unwind: "$person",
  },
  {
    $match: {
      "person.e_mail":
        "jnlpu_b07559ff04737ce21a10bb8a5438ac62@example.com",
    },
  },
  {
    $project: {
      _id: 0,
      end_time: 1,
    },
  },
])

//3_1
db.person.aggregate([
    {
      $lookup: {
        from: 'country',
        localField: 'country_code',
        foreignField: 'country_code',
        as: 'country'
      }
    },
    { $unwind: '$country' },
    {
      $group: {
        _id: '$country_code',
        country_name: { $first: '$country.name' },
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
    { $sort: { _id: 1 } }
])

//3_2
db.employment.aggregate([
    {
      $match: {
        start_time: { $exists: true },
        end_time: { $exists: true }
      }
    },
    {
      $lookup: {
        from: 'occupation',
        localField: 'occupation_code',
        foreignField: 'occupation_code',
        as: 'occupation'
      }
    },
    { $unwind: '$occupation' },
    {
      $group: {
        _id: '$occupation_code',
        occupation_name: {
          $first: '$occupation.name'
        },
        employment_count: { $sum: 1 },
        avg_duration_days: {
          $avg: {
            $divide: [
              {
                $subtract: [
                  '$end_time',
                  '$start_time'
                ]
              },
              86400000
            ]
          }
        }
      }
    },
    { $sort: { _id: 1 } }
])

//3_3

db.person.aggregate([
  {
    $lookup: {
      from: "employment",
      localField: "_id",
      foreignField: "person_id",
      as: "employments"
    }
  },
  {
    $project: {
      _id: 1,
      e_mail: 1,
      employment_count: { $size: "$employments" }
    }
  }
]);

//4_1
db.person.aggregate([
  {
    $lookup: {
      from: "employee",
      localField: "_id",
      foreignField: "person_id",
      as: "employee",
    },
  },
  {
    $match: {
      employee: {
        $ne: [],
      },
    },
  },
  {
    $lookup: {
      from: "employee_status_type",
      localField:
        "employee.employee_status_type_code",
      foreignField: "employee_status_type_code",
      as: "employee_status",
    },
  },
  {
    $lookup: {
      from: "employment",
      localField: "_id",
      foreignField: "person_id",
      as: "employments",
    },
  },
  {
    $project: {
      e_mail: 1,
      surname: 1,
      "employee_status.name": 1,
      employments: 1,
    },
  },
]);

//4_2

db.employment.aggregate([
  {
    $lookup: {
      from: "occupation",
      localField: "occupation_code",
      foreignField: "occupation_code",
      as: "occupation"
    }
  },
  {
    $lookup: {
      from: "person",
      localField: "person_id",
      foreignField: "_id",
      as: "person"
    }
  },
  {
    $project: {
      start_time: 1,
      end_time: 1,
      "occupation.name": 1,
      "person.e_mail": 1,
      "person.surname": 1
    }
  }
]);

//5_1

db.person.aggregate([
  {
        $lookup: {
            from: "employee",
            localField: "_id",
            foreignField: "person_id",
            as: "employee"
        }
    },
    {
        $unwind: "$employee"
    },
    {
        $lookup: {
            from: "employee_status_type",
            localField: "employee.employee_status_type_code",
            foreignField: "employee_status_type_code",
            as: "employee_status"
        }
    },
    {
        $lookup: {
            from: "person",
            localField: "employee.mentor_id",
            foreignField: "_id",
            as: "mentor"
        }
    },
    {
        $unwind: "$mentor"
    },
    {
        $lookup: {
            from: "employee",
            localField: "mentor._id",
            foreignField: "person_id",
            as: "mentor_employee"
        }
    },
    {
        $unwind: "$mentor_employee"
    },
    {
        $lookup: {
            from: "employee_status_type",
            localField: "mentor_employee.employee_status_type_code",
            foreignField: "employee_status_type_code",
            as: "mentor_status"
        }
    },
    {
        $match: {
            $expr: {
                $ne: ["$employee.employee_status_type_code", "$mentor_employee.employee_status_type_code"]
            }
        }
    },
    {
        $project: {
          _id: 0,
            employee_email: "$e_mail",
            employee_status: { $arrayElemAt: ["$employee_status.name", 0] },
            mentor_email: "$mentor.e_mail",
            mentor_status: { $arrayElemAt: ["$mentor_status.name", 0] }
        }
    }
])

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





