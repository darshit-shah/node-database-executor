const queryExecutor = require("node-database-executor").promise;
const utils = require("axiom-utils");
const fs = require("fs");
const d3 = require("d3");
async function runQuery() {
  try {
    const raw_data_databaseType_JSON = "json",
      raw_data_file_path = "./data/",
      raw_data_file_name = "sampleData1.csv";

    const fileData1 = await fs.readFileSync(raw_data_file_path + "/" + raw_data_file_name, "utf8");
    const dataSet1 = utils.CSV2JSON(fileData1, null, null, null, null, '"');

    var dbConfig = {
      type: "database", //type of connection. Currently connection to only database is available
      databaseType: raw_data_databaseType_JSON //type of database. Currently you can connect to only mysql database
      //   data:data
    };
    let query = {
      table: dataSet1,
      select: [
        {
          field: "serialNumber",
          alias: "sno"
        },
        {
          field: "districtName"
        }
      ]
    };
    let queryConfig = {
      dbConfig: dbConfig,
      query: query
    };
    let jsonQueryData = null;

    //   jsonQueryData = await queryExecutor.executeQuery(queryConfig);
    //   console.log("jsonQueryData:",jsonQueryData[0]);

    query = {
      table: dataSet1,
      select: [
        {
          field: "treatmentNoOfChloroquineTablets",
          alias: "ChloroquineTablets"
        },
        {
          field: "districtName"
        },
        {
          field: "treatmentNoOfChloroquineTablets",
          aggregation: "count"
        },
        {
          field: "treatmentNoOfChloroquineTablets",
          aggregation: "sum"
        },
        {
          field: "treatmentNoOfChloroquineTablets",
          aggregation: "avg"
        },
        {
          field: "treatmentNoOfChloroquineTablets",
          aggregation: "min"
        },
        {
          field: "treatmentNoOfChloroquineTablets",
          aggregation: "max"
        }
      ]
    };
    queryConfig = {
      dbConfig: dbConfig,
      query: query
    };

    //   jsonQueryData = await queryExecutor.executeQuery(queryConfig);
    //   select treatmentNoOfChloroquineTablets as ChloroquineTablets,districtName,count(CAST(treatmentNoOfChloroquineTablets as int)),sum(treatmentNoOfChloroquineTablets),
    //   avg(treatmentNoOfChloroquineTablets),min(treatmentNoOfChloroquineTablets),
    //   max(treatmentNoOfChloroquineTablets)
    //   from tableName where serialNumber>=1 and serialNumber<=1000;
    //   console.log("jsonQueryData with aggregation:",jsonQueryData);

    query = {
      table: dataSet1,
      alias: "SM",
      select: [
        {
          field: "treatmentNoOfChloroquineTablets",
          alias: "ChloroquineTablets"
        },
        {
          field: "gender"
        },
        {
          field: "treatmentNoOfChloroquineTablets",
          aggregation: "count"
        },
        {
          field: "treatmentNoOfChloroquineTablets",
          aggregation: "sum"
        },
        {
          field: "treatmentNoOfChloroquineTablets",
          aggregation: "avg"
        },
        {
          field: "treatmentNoOfChloroquineTablets",
          aggregation: "min"
        },
        {
          field: "treatmentNoOfChloroquineTablets",
          aggregation: "max"
        }
      ],
      groupby: {
        table: "SM",
        field: "gender"
      }
    };
    queryConfig = {
      dbConfig: dbConfig,
      query: query
    };

    //   jsonQueryData = await queryExecutor.executeQuery(queryConfig);
    //   console.log("jsonQueryData with aggregation and group by:",jsonQueryData);
    //   select gender,count(CAST(treatmentNoOfChloroquineTablets as int)),sum(treatmentNoOfChloroquineTablets),
    //   avg(treatmentNoOfChloroquineTablets),min(treatmentNoOfChloroquineTablets),
    //   max(treatmentNoOfChloroquineTablets)
    //   from tableName where serialNumber>=1 and serialNumber<=1000 group by gender;

    query = {
      table: dataSet1,
      select: [
        {
          field: "gender",
          alias: "Gender"
        },
        {
          field: "treatmentNoOfChloroquineTablets",
          aggregation: "count"
        },
        {
          field: "treatmentNoOfChloroquineTablets",
          aggregation: "sum"
        },
        {
          field: "treatmentNoOfChloroquineTablets",
          aggregation: "avg"
        },
        {
          field: "treatmentNoOfChloroquineTablets",
          aggregation: "min"
        },
        {
          field: "treatmentNoOfChloroquineTablets",
          aggregation: "max"
        },
        {
          field: "gender",
          aggregation: "distinct"
        },
        {
          field: "districtName",
          aggregation: "distinct"
        }
      ],
      groupby: {
        table: "SM",
        field: "gender"
      }
    };
    queryConfig = {
      dbConfig: dbConfig,
      query: query
    };

    //   jsonQueryData = await queryExecutor.executeQuery(queryConfig);
    // CBT:select gender,count(treatmentNoOfChloroquineTablets),sum(treatmentNoOfChloroquineTablets),avg(treatmentNoOfChloroquineTablets),
    // min(treatmentNoOfChloroquineTablets),max(treatmentNoOfChloroquineTablets),
    // districtName,gender
    // from tableName where serialNumber>=1 and serialNumber<=1000 group by gender;

    //   console.log("jsonQueryData with aggregation distinct:",jsonQueryData);

    query = {
      table: dataSet1,
      select: [
        {
          field: "gender",
          alias: "Gender"
        },
        {
          field: "treatmentNoOfChloroquineTablets",
          aggregation: "count"
        },
        {
          field: "treatmentNoOfChloroquineTablets",
          aggregation: "sum"
        },
        {
          field: "treatmentNoOfChloroquineTablets",
          aggregation: "avg"
        },
        {
          field: "treatmentNoOfChloroquineTablets",
          aggregation: "min"
        },
        {
          field: "treatmentNoOfChloroquineTablets",
          aggregation: "max"
        },
        {
          field: "districtName",
          aggregation: ["count", "distinct"],
          alias: "countOfDistrict"
        },
        {
          field: "districtName",
          aggregation: "distinct"
        }
      ],
      groupby: {
        table: "SM",
        field: "gender"
      }
    };
    queryConfig = {
      dbConfig: dbConfig,
      query: query
    };

    //   jsonQueryData = await queryExecutor.executeQuery(queryConfig);
    //   // CBT:select gender,count(treatmentNoOfChloroquineTablets),sum(treatmentNoOfChloroquineTablets),avg(treatmentNoOfChloroquineTablets),min(treatmentNoOfChloroquineTablets),max(treatmentNoOfChloroquineTablets),count(distinct(districtName)) as countOfDistrict,distinct(districtName) from table group by  gender;
    //   console.log("jsonQueryData with aggregation count(distinct(district)):",jsonQueryData);

    query = {
      table: dataSet1,
      select: [
        {
          field: "gender",
          alias: "Gender"
        },
        {
          field: "treatmentNoOfChloroquineTablets",
          aggregation: "count"
        },
        {
          field: "treatmentNoOfChloroquineTablets",
          aggregation: "sum"
        },
        {
          field: "treatmentNoOfChloroquineTablets",
          aggregation: "avg"
        },
        {
          field: "treatmentNoOfChloroquineTablets",
          aggregation: "min"
        },
        {
          field: "treatmentNoOfChloroquineTablets",
          aggregation: "max"
        },
        {
          field: "districtName",
          aggregation: ["count", "distinct"],
          alias: "countOfDistrict"
        },
        {
          field: "districtName",
          aggregation: "distinct"
        }
      ],
      groupby: [
        {
          table: "SM",
          field: "districtName"
        },
        {
          table: "SM",
          field: "gender"
        }
      ]
    };
    queryConfig = {
      dbConfig: dbConfig,
      query: query
    };

    //   jsonQueryData = await queryExecutor.executeQuery(queryConfig);
    //   // CBT:select gender,districtName,count(treatmentNoOfChloroquineTablets),sum(treatmentNoOfChloroquineTablets),avg(treatmentNoOfChloroquineTablets),min(treatmentNoOfChloroquineTablets),
    // max(treatmentNoOfChloroquineTablets),
    // count(districtName) as countOfDistrict
    // from tableName where serialNumber>=1 and serialNumber<=1000
    // group by districtName,gender;
    //   console.log("jsonQueryData with aggregation count(distinct(district)):",jsonQueryData);

    query = {
      table: dataSet1,
      select: [
        {
          field: "gender",
          alias: "Gender"
        },
        {
          field: "treatmentNoOfChloroquineTablets",
          aggregation: "count"
        },
        {
          field: "treatmentNoOfChloroquineTablets",
          aggregation: "sum"
        },
        {
          field: "treatmentNoOfChloroquineTablets",
          aggregation: "avg"
        },
        {
          field: "treatmentNoOfChloroquineTablets",
          aggregation: "min"
        },
        {
          field: "treatmentNoOfChloroquineTablets",
          aggregation: "max"
        },
        {
          field: "districtName",
          aggregation: ["count", "distinct"],
          alias: "countOfDistrict"
        },
        {
          field: "districtName",
          aggregation: "distinct"
        }
      ],
      groupby: [
        {
          table: "SM",
          field: "districtName"
        },
        {
          table: "SM",
          field: "gender"
        }
      ],
      // filter: {
      //     field: 'gender',
      //     operator: 'eq',
      //     value: 'M'
      // },
      // select gender,districtName,count(CAST(treatmentNoOfChloroquineTablets as int)),sum(treatmentNoOfChloroquineTablets),avg(treatmentNoOfChloroquineTablets),min(treatmentNoOfChloroquineTablets),
      // max(treatmentNoOfChloroquineTablets),count(distinct(districtName)) as countOfDistrict
      // from tableName where serialNumber>=1 and serialNumber<=1000 and (gender like 'M')
      // group by  districtName,gender order by CAST(serialNumber as int);
      // filter: {
      //    AND:[{
      //        or:[{
      //         field: 'gender',
      //         operator: 'eq',
      //         value: ['M','F']
      //     }]},
      //     {
      //         field: 'age',
      //         operator: 'eq',
      //         value: 25
      //     }],

      // },
      // select gender,districtName,count(CAST(treatmentNoOfChloroquineTablets as int)),sum(treatmentNoOfChloroquineTablets),avg(treatmentNoOfChloroquineTablets),min(treatmentNoOfChloroquineTablets),
      // max(treatmentNoOfChloroquineTablets),count(distinct(districtName)) as countOfDistrict
      // from tableName where serialNumber>=1 and serialNumber<=1000 and ((gender like 'M' or gender like 'F')  and age='25')
      // group by  districtName,gender order by CAST(serialNumber as int);
      // filter: {
      //     field: 'gender',
      //     operator: 'noteq',
      //     value: ['M','F']
      // },
      // select gender,districtName,count(CAST(treatmentNoOfChloroquineTablets as int)),sum(treatmentNoOfChloroquineTablets),avg(treatmentNoOfChloroquineTablets),min(treatmentNoOfChloroquineTablets),
      // max(treatmentNoOfChloroquineTablets),count(distinct(districtName)) as countOfDistrict
      // from tableName where serialNumber>=1 and serialNumber<=1000 and (gender not like 'M' and gender not like 'F')
      // group by  districtName,gender order by CAST(serialNumber as int);
      // filter: {
      //     or: [{
      //       field: 'gender',
      //       operator: 'EQ',
      //       value: 'M'
      //     },{
      //         field: 'districtName',
      //         operator: 'EQ',
      //         value: 'Bijapur'
      //     }]
      // },
      // select gender,districtName,count(CAST(treatmentNoOfChloroquineTablets as int)),sum(treatmentNoOfChloroquineTablets),avg(treatmentNoOfChloroquineTablets),min(treatmentNoOfChloroquineTablets),
      // max(treatmentNoOfChloroquineTablets),count(distinct(districtName)) as countOfDistrict
      // from tableName where serialNumber>=1 and serialNumber<=1000 and (gender like 'M' or districtName like 'Bijapur')
      // group by  districtName,gender order by CAST(serialNumber as int) limit 1000;
      // filter: {
      //     AND: [
      //         {
      //          or:[{
      //             field: 'gender',
      //             operator: 'EQ',
      //             value: 'M'
      //          },{
      //             field: 'gender',
      //             operator: 'EQ',
      //             value: 'F'
      //          }]
      //      },{
      //         field: 'districtName',
      //         operator: 'EQ',
      //         value: 'Bijapur'
      //     }]
      // },
      // select gender,districtName,count(CAST(treatmentNoOfChloroquineTablets as int)),sum(treatmentNoOfChloroquineTablets),avg(treatmentNoOfChloroquineTablets),min(treatmentNoOfChloroquineTablets),
      // max(treatmentNoOfChloroquineTablets),count(distinct(districtName)) as countOfDistrict
      // from tableName where serialNumber>=1 and serialNumber<=1000 and (gender like 'M' or gender like 'F') and districtName like 'Bijapur'
      // group by  districtName,gender order by CAST(serialNumber as int) limit 1000;
      // filter: {
      //     or: [
      //         {
      //          and:[{
      //             field: 'gender',
      //             operator: 'EQ',
      //             value: 'M'
      //          },{
      //             field: 'districtName',
      //             operator: 'EQ',
      //             value: 'Bijapur'
      //          }]
      //      },{
      //         field: 'gender',
      //         operator: 'EQ',
      //         value: 'F'
      //     }]
      // },
      // select gender,districtName,count(CAST(treatmentNoOfChloroquineTablets as int)),sum(treatmentNoOfChloroquineTablets),avg(treatmentNoOfChloroquineTablets),min(treatmentNoOfChloroquineTablets),
      // max(treatmentNoOfChloroquineTablets),count(distinct(districtName)) as countOfDistrict
      // from tableName where serialNumber>=1 and serialNumber<=1000 and ((gender like 'M' and districtName like 'Bijapur' ) or gender like 'F')
      // group by  districtName,gender order by CAST(serialNumber as int) limit 1000;
      filter: {
        or: [
          {
            and: [
              {
                field: "gender",
                operator: "EQ",
                value: "M"
              },
              {
                field: "districtName",
                operator: "EQ",
                value: "Bijapur"
              }
            ]
          },
          {
            and: [
              {
                field: "gender",
                operator: "EQ",
                value: "F"
              },
              {
                field: "districtName",
                operator: "EQ",
                value: "Sukma"
              }
            ]
          }
        ]
      }
      // select gender,districtName,count(CAST(treatmentNoOfChloroquineTablets as int)),sum(treatmentNoOfChloroquineTablets),avg(treatmentNoOfChloroquineTablets),min(treatmentNoOfChloroquineTablets),
      // max(treatmentNoOfChloroquineTablets),count(distinct(districtName)) as countOfDistrict
      // from tableName where serialNumber>=1 and serialNumber<=1000 and ((gender like 'M' and districtName like 'Bijapur' ) or (gender like 'F' and districtName like 'Sukma' ))
      // group by  districtName,gender order by CAST(serialNumber as int) limit 1000;
    };
    queryConfig = {
      dbConfig: dbConfig,
      query: query
    };

    //   jsonQueryData = await queryExecutor.executeQuery(queryConfig);
    //   // CBT:select gender,count(treatmentNoOfChloroquineTablets),sum(treatmentNoOfChloroquineTablets),avg(treatmentNoOfChloroquineTablets),min(treatmentNoOfChloroquineTablets),max(treatmentNoOfChloroquineTablets),count(distinct(districtName)) as countOfDistrict,districtName from table where gender like 'M' group by  districtName,gender;
    //   console.log("jsonQueryData with aggregation  with filter count(distinct(district)):",jsonQueryData);

    query = {
      table: dataSet1,
      select: [
        {
          field: "gender",
          alias: "Gender"
        },
        {
          field: "treatmentNoOfChloroquineTablets",
          aggregation: "count",
          alias: "total"
        },
        {
          field: "treatmentNoOfChloroquineTablets",
          aggregation: "sum"
        },
        {
          field: "treatmentNoOfChloroquineTablets",
          aggregation: "avg"
        },
        {
          field: "treatmentNoOfChloroquineTablets",
          aggregation: "min"
        },
        {
          field: "treatmentNoOfChloroquineTablets",
          aggregation: "max"
        },
        {
          field: "districtName",
          aggregation: ["count", "distinct"],
          alias: "countOfDistrict"
        },
        {
          field: "districtName",
          aggregation: "distinct"
        }
      ],
      groupby: [
        {
          table: "SM",
          field: "districtName"
        },
        {
          table: "SM",
          field: "gender"
        }
      ],
      having: {
        field: "total",
        operator: "gteq",
        value: 110
      }
    };
    queryConfig = {
      dbConfig: dbConfig,
      query: query
    };

    //   jsonQueryData = await queryExecutor.executeQuery(queryConfig);
    // CBT:select gender,count(treatmentNoOfChloroquineTablets) as total,sum(treatmentNoOfChloroquineTablets),avg(treatmentNoOfChloroquineTablets),min(treatmentNoOfChloroquineTablets),max(treatmentNoOfChloroquineTablets),count(distinct(districtName)) as countOfDistrict,districtName from tableName where serialNumber>=1 and serialNumber<=1000 group by  districtName,gender having total>=110 ;
    //   console.log("jsonQueryData with aggregation count(distinct(district)) having:",JSON.stringify(jsonQueryData));

    query = {
      table: dataSet1,
      select: [
        {
          field: "gender",
          alias: "Gender"
        },
        {
          field: "treatmentNoOfChloroquineTablets",
          aggregation: "count",
          alias: "total"
        },
        {
          field: "treatmentNoOfChloroquineTablets",
          aggregation: "sum",
          alias: "summation"
        },
        {
          field: "treatmentNoOfChloroquineTablets",
          aggregation: "avg"
        },
        {
          field: "treatmentNoOfChloroquineTablets",
          aggregation: "min"
        },
        {
          field: "treatmentNoOfChloroquineTablets",
          aggregation: "max"
        },
        {
          field: "districtName",
          aggregation: ["count", "distinct"],
          alias: "countOfDistrict"
        },
        {
          field: "districtName",
          aggregation: "distinct"
        }
      ],
      groupby: [
        {
          table: "SM",
          field: "districtName"
        },
        {
          table: "SM",
          field: "gender"
        }
      ],
      having: {
        AND: [
          {
            field: "total",
            operator: "gteq",
            value: 110
          },
          {
            field: "summation",
            operator: "gteq",
            value: 800
          }
        ]
      }
    };
    queryConfig = {
      dbConfig: dbConfig,
      query: query
    };

    //   jsonQueryData = await queryExecutor.executeQuery(queryConfig);
    // CBT:select gender,count(treatmentNoOfChloroquineTablets) as total,sum(treatmentNoOfChloroquineTablets) as summation,avg(treatmentNoOfChloroquineTablets),min(treatmentNoOfChloroquineTablets),max(treatmentNoOfChloroquineTablets),count(distinct(districtName)) as countOfDistrict,districtName from tableName where serialNumber>=1 and serialNumber<=1000 group by  districtName,gender having total>=110 and summation>=800;
    //   console.log("jsonQueryData with aggregation count(distinct(district)) having:",(jsonQueryData));

    query = {
      table: dataSet1,
      select: [
        {
          field: "serialNumber",
          alias: "sno"
        },
        {
          field: "districtName"
        }
      ],
      sortby: [
        {
          field: "districtName",
          order: "asc"
        }
      ],
      filter: {
        AND: [
          {
            field: "serialNumber",
            operator: "gteq",
            value: 1
          },
          {
            field: "serialNumber",
            operator: "lteq",
            value: 10
          }
        ]
      }
    };
    queryConfig = {
      dbConfig: dbConfig,
      query: query
    };
    //   jsonQueryData = await queryExecutor.executeQuery(queryConfig);
    //CBT:select serialNumber,districtName from tableName where serialNumber>=1 and serialNumber<=10 order by serialNumber ,districtName ;
    //   console.log("jsonQueryData:",jsonQueryData);

    const leftTableData = await fs.readFileSync(raw_data_file_path + "/sampleData2.csv", "utf8");
    const rightTableData = await fs.readFileSync(raw_data_file_path + "/sampleData3.csv", "utf8");
    const dataSetLeft = utils.CSV2JSON(leftTableData, null, null, null, null, '"');
    const dataSetRight = utils.CSV2JSON(rightTableData, null, null, null, null, '"');

    query = {
      join: {
        table: dataSetLeft,
        alias: "mview",
        joinwith: [
          {
            table: dataSetRight,
            alias: "mtab",
            type: "inner",
            joincondition: {
              table: "mview",
              field: "serialNumber",
              operator: "eq",
              value: {
                table: "mtab",
                field: "serialNumber"
              }
            }
          }
        ]
      },
      select: [
        {
          field: "serialNumber",
          table: "mview"
        },
        {
          field: "districtName",
          table: "mview",
          alias: "districtNameview"
        },
        {
          field: "age",
          table: "mview",
          alias: "ageview"
        },
        {
          field: "gender",
          table: "mview",
          alias: "genderview"
        },
        {
          field: "blockName",
          table: "mtab",
          alias: "blockNametab"
        },
        {
          field: "primaryHealthCentreName",
          table: "mtab",
          alias: "primaryHealthCentreNametab"
        }
      ],
      sortby: [
        {
          field: "serialNumber",
          order: "asc"
        }
      ],
      filter: {
        AND: [
          {
            field: "serialNumber",
            table: "mview",
            operator: "gteq",
            value: 1
          },
          {
            field: "serialNumber",
            table: "mview",
            operator: "lteq",
            value: 3
          }
        ]
      }
    };
    queryConfig = {
      dbConfig: dbConfig,
      query: query
    };
    //   jsonQueryData = await queryExecutor.executeQuery(queryConfig);
    //   console.log("jsonQueryData Inner join:",(jsonQueryData));

    query = {
      join: {
        table: dataSetLeft,
        alias: "mview",
        joinwith: [
          {
            table: dataSetRight,
            alias: "mtab",
            type: "left",
            joincondition: {
              table: "mview",
              field: "serialNumber",
              operator: "eq",
              value: {
                table: "mtab",
                field: "serialNumber"
              }
            }
          }
        ]
      },
      select: [
        {
          field: "serialNumber",
          table: "mview"
        },
        {
          field: "districtName",
          table: "mview",
          alias: "districtNameview"
        },
        {
          field: "age",
          table: "mview",
          alias: "ageview"
        },
        {
          field: "gender",
          table: "mview",
          alias: "genderview"
        },
        {
          field: "blockName",
          table: "mtab",
          alias: "blockNametab"
        },
        {
          field: "primaryHealthCentreName",
          table: "mtab",
          alias: "primaryHealthCentreNametab"
        }
      ],
      sortby: [
        {
          field: "serialNumber",
          order: "asc"
        }
      ]
      // filter:{
      //     AND:[{
      //         field:"serialNumber",
      //         operator:"gteq",
      //         value:1
      //     },{
      //         field:"serialNumber",
      //         operator:"lteq",
      //         value:10
      //     }]
      // }
    };
    queryConfig = {
      dbConfig: dbConfig,
      query: query
    };
    //   jsonQueryData = await queryExecutor.executeQuery(queryConfig);
    //   console.log("jsonQueryData left join:",JSON.stringify(jsonQueryData));

    query = {
      join: {
        table: dataSetLeft,
        alias: "mview",
        joinwith: [
          {
            table: dataSetRight,
            alias: "mtab",
            type: "right",
            joincondition: {
              table: "mview",
              field: "serialNumber",
              operator: "eq",
              value: {
                table: "mtab",
                field: "serialNumber"
              }
            }
          }
        ]
      },
      select: [
        {
          field: "serialNumber",
          table: "mtab"
        },
        {
          field: "districtName",
          table: "mview",
          alias: "districtNameview"
        },
        {
          field: "age",
          table: "mview",
          alias: "ageview"
        },
        {
          field: "gender",
          table: "mview",
          alias: "genderview"
        },
        {
          field: "blockName",
          table: "mtab",
          alias: "blockNametab"
        },
        {
          field: "primaryHealthCentreName",
          table: "mtab",
          alias: "primaryHealthCentreNametab"
        }
      ],
      sortby: [
        {
          field: "serialNumber",
          order: "asc"
        }
      ],
      filter: {
        or: [
          {
            AND: [
              {
                field: "serialNumber",
                table: "mtab",
                operator: "gteq",
                value: 1
              },
              {
                field: "serialNumber",
                table: "mtab",
                operator: "lteq",
                value: 3
              }
            ]
          },
          {
            field: "serialNumber",
            table: "mtab",
            operator: "gteq",
            value: 13
          }
        ]
      }
    };
    queryConfig = {
      dbConfig: dbConfig,
      query: query
    };
    //   jsonQueryData = await queryExecutor.executeQuery(queryConfig);
    //   console.log("jsonQueryData right join:",JSON.stringify(jsonQueryData));

    query = {
      join: {
        table: dataSetLeft,
        alias: "mview",
        joinwith: [
          {
            table: dataSetRight,
            alias: "mtab",
            type: "right",
            joincondition: {
              AND: [
                {
                  table: "mview",
                  field: "serialNumber",
                  operator: "eq",
                  value: {
                    table: "mtab",
                    field: "serialNumber"
                  }
                },
                {
                  table: "mview",
                  field: "districtName",
                  operator: "eq",
                  value: {
                    table: "mtab",
                    field: "districtName"
                  }
                }
              ]
            }
          }
        ]
      },
      select: [
        {
          field: "serialNumber",
          table: "mtab"
        },
        {
          field: "districtName",
          table: "mview",
          alias: "districtNameview"
        },
        {
          field: "age",
          table: "mview",
          alias: "ageview"
        },
        {
          field: "gender",
          table: "mview",
          alias: "genderview"
        },
        {
          field: "blockName",
          table: "mtab",
          alias: "blockNametab"
        },
        {
          field: "primaryHealthCentreName",
          table: "mtab",
          alias: "primaryHealthCentreNametab"
        }
      ],
      sortby: [
        {
          field: "serialNumber",
          order: "asc"
        }
      ]
    };
    queryConfig = {
      dbConfig: dbConfig,
      query: query
    };
    //   jsonQueryData = await queryExecutor.executeQuery(queryConfig);
    //   console.log("jsonQueryData inner join:",JSON.stringify(jsonQueryData));
  } catch (error) {
    console.log(error);
  }
}

setTimeout(function() {
  runQuery();
}, 1000);
