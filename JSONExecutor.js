let debug = require("debug")("database-executor:mysql-executor");
const d3 = require("d3");
const d3Array = require("d3-array");
const operatorrToFunation = {
  "==": (a, b) => {
    return a == b;
  },
  "!=": (a, b) => {
    return a != b;
  },
  ">": (a, b) => {
    return a > b;
  },
  "<": (a, b) => {
    return a < b;
  },
  ">=": (a, b) => {
    return a >= b;
  },
  "<=": (a, b) => {
    return a <= b;
  },
  indexOf: ( value,data) => {
    return data.indexOf(value) != -1;
  },
  "!indexOf": (value,data) => {
    return data.indexOf(value) == -1;
  }
};

function executeQuery(connection, rawQuery, cb) {
  //First step filter
  //join
  //group by
  //having
  //sort
  //select
  let result = [];
  let tableAlias=[];
  //CBT:apply filter on data
  if (rawQuery.hasOwnProperty("filter") && !rawQuery.hasOwnProperty("join")) {
    rawQuery.table = rawQuery.table.filter(row => {
      return processFilter(rawQuery.filter, row,tableAlias);
    });
  }

  //CBT:apply join on tables
  if(rawQuery.hasOwnProperty("join")) {
    const resultJoin=prepareDatafromJoin(rawQuery.join,rawQuery.select);
    rawQuery.table=resultJoin.resultData;
    tableAlias=resultJoin.tableAlias;
    if (rawQuery.hasOwnProperty("filter")) {
      rawQuery.table = rawQuery.table.filter(row => {
        return processFilter(rawQuery.filter, row,tableAlias);
      });
    }
  }



  //CBT:get data from array
  if (rawQuery.hasOwnProperty("select")) {
    //CBT:if want to select all record with all columns
    if (rawQuery.select.length == 0) {
      result = rawQuery.table;
    } else {
      const fieldWithAggregation = rawQuery.select.filter(data => data.hasOwnProperty("aggregation"));
      const fieldWithoutAggregation = rawQuery.select.filter(data => !data.hasOwnProperty("aggregation"));
      if(fieldWithAggregation.length>0){
        //CBT:if query don't have any group by
        if (rawQuery.groupby==null  || (rawQuery.groupby!=null && Array.isArray(rawQuery.groupby)==true && rawQuery.groupby.length==0)) {
          // const aggregatedData = getAggregationData(rawQuery.table, rawQuery.select);
          // //CBT:select all fields from select which doesn't have aggregation key
          // const fieldWithoutAggregation = rawQuery.select.filter(data => !data.hasOwnProperty("aggregation"));
          // result = getDataByFieldName(rawQuery.table, fieldWithoutAggregation, aggregatedData,tableAlias);

          
          result = [processAggregation(null, rawQuery.table, fieldWithoutAggregation, fieldWithAggregation)];
        } else {
          result = getGroupByData(rawQuery.table, rawQuery.select, rawQuery.groupby,tableAlias);
        }
      }else{
        result=processSelect(rawQuery.table, fieldWithoutAggregation)
      }
     
    }
  }

  //CBT:apply filter on data if rawQuery have having
  if (rawQuery.hasOwnProperty("having")) {
    result = result.filter(row => {
      return processFilter(rawQuery.having, row,tableAlias);
    });
  }

  //CBT:sort by
  if(rawQuery.hasOwnProperty("sortby")){
    rawQuery.sortby.map(sortdata=>{
      const sortOrder = sortdata.order ? sortdata.order.toString().toLowerCase() : 'asc';
      const sortField = sortdata.field;
      const encloseField=sortdata.encloseField;
      checkEncloseField(encloseField);
      result=result.sort((a,b)=>{
          if(sortOrder=='asc' && a[sortField]>b[sortField])
            return 1;
          else if(sortOrder=='asc' && a[sortField]<[sortField])
            return -1;
          if(sortOrder=='desc' && a[sortField]>b[sortField])
            return -1;
          else if(sortOrder=='desc' && a[sortField]<[sortField])
            return 1;
          else 
            return 0;
      })
    })
  }
  cb({
    status: true,
    content: result
  });
}

function prepareDatafromJoin(rawQueryJoin,rawQuerySelect){
  const tableAlias=[rawQueryJoin.alias];
  const leftMostTableData=rawQueryJoin.table;
  const leftMostTableDataAlias=rawQueryJoin.alias;
  const resultData=[];
  const joinwith=rawQueryJoin.joinwith;
  const tableDataObj={};
  if(joinwith!=null && Array.isArray(joinwith)){
    joinwith.forEach(joinWithData=>{
      tableAlias.push(joinWithData.alias);
      const rightMostTableData=joinWithData.table;
      const rightMostTableDataAlias=joinWithData.alias;
      joinWithData.type=joinWithData.type || 'inner';
      const typeOfJoin=joinWithData.type.toString().toLowerCase();
      const joincondition=joinWithData.joincondition;
      const tableData={
        [leftMostTableDataAlias]:[...leftMostTableData],
        [rightMostTableDataAlias]:[...rightMostTableData]
      }
      const joinWiseData=processJoinCondition(tableData,typeOfJoin,joincondition);
      joinWiseData.forEach(data=>{
        resultData.push(data);
      });
    })
  }
  return {resultData:resultData,tableAlias:tableAlias};
}

function processSelect(rawData,fieldWithoutAggregation){
  const result=[];
  for(let i=0;i<rawData.length;i++){
    const selectObj = {};
    fieldWithoutAggregation.forEach(select=>{
      selectObj[select.alias || select.field] = rawData[i][select.field];
    });

    result.push(selectObj);
  }
  return result;
}

function processAggregation(arrIndex, rawData, fieldWithoutAggregation, fieldWithAggregation){
  const aggrObj = {};
  fieldWithoutAggregation.forEach(select=>{
    aggrObj[select.alias || select.field] = rawData[((arrIndex && arrIndex[0]) || 0)][select.field];
  })
  fieldWithAggregation.forEach(aggr=>{
    let output=null;
    if(aggr.aggregation.toLowerCase() === 'count'){
      output = (arrIndex && arrIndex.length) || rawData.length;
    } else if(aggr.aggregation.toLowerCase() === 'sum') {
      output = 0;
      for(let i=0;i<((arrIndex && arrIndex.length) || rawData.length);i++){
        if(rawData[((arrIndex && arrIndex[i]) || i)][aggr.field] != null){
          output += +rawData[((arrIndex && arrIndex[i]) || i)][aggr.field];
        }
      }
    }
    else if(aggr.aggregation.toLowerCase() === 'min') {
      output = +Infinity;
      for(let i=0;i<((arrIndex && arrIndex.length) || rawData.length);i++){
        if(rawData[((arrIndex && arrIndex[i]) || i)][aggr.field] != null){
          if(output> +rawData[((arrIndex && arrIndex[i]) || i)][aggr.field])
            output = +rawData[((arrIndex && arrIndex[i]) || i)][aggr.field];
        }
      }
    }
    else if(aggr.aggregation.toLowerCase() === 'max') {
      output = -Infinity;
      for(let i=0;i<((arrIndex && arrIndex.length) || rawData.length);i++){
        if(rawData[((arrIndex && arrIndex[i]) || i)][aggr.field] != null){
          if(output< +rawData[((arrIndex && arrIndex[i]) || i)][aggr.field])
            output = +rawData[((arrIndex && arrIndex[i]) || i)][aggr.field];
        }
      }
    }
    else if(aggr.aggregation.toLowerCase() === 'average' || aggr.aggregation.toLowerCase() === 'avg' || aggr.aggregation.toLowerCase() === 'mean'){
      output = 0;
      let counter = 0;
      for(let i=0;i<((arrIndex && arrIndex.length) || rawData.length);i++){
        if(rawData[((arrIndex && arrIndex[i]) || i)][aggr.field] != null){
          output += +rawData[((arrIndex && arrIndex[i]) || i)][aggr.field];
          counter++;
        }
      }
      output /= counter;
    } else {
      throw Error("Unknown aggregation type");
    }
    aggrObj[aggr.alias || (aggr.aggregation+"("+aggr.field+")")] = output
  })
  return aggrObj;
}

function getGroupByData(rawData, select, groupby,tableAlias) {
  let result = [];
  const fieldWithoutAggregation = select.filter(data => !data.hasOwnProperty("aggregation"));
  const fieldWithAggregation = select.filter(data => data.hasOwnProperty("aggregation"));
  if (Array.isArray(groupby) && groupby.length === 1) {
    groupby = groupby[0];
  }

  const joiningKey = '#$%%$#';
  for(let i=0;i<rawData.length;i++){
    let key = null;
    if(Array.isArray(groupby)){
      let key = [];
      for(let g=0;g<groupby.length;g++){
        key.push(rawData[i][groupby[g].field])
      }
      key = key.join(joiningKey)
    } else {
      key = rawData[i][groupby.field]
    }
    if(!result[key]){
      result[key]=[];
    }
    result[key].push(i);
  }

  result = Object.keys(result).map(key=>{
    const obj = processAggregation(result[key], rawData, fieldWithoutAggregation, fieldWithAggregation);
    return obj; 
  })
  return result;

  // if(Array.isArray(groupby)) {
  //   // CBT:if group by is an array
  //   result = d3
  //     .nest()
  //     .key(keyData => {
  //       return groupby
  //         .map(data => {
  //           return keyData[data.field];
  //         })
  //         .join("#$%%$#");
  //     })
  //     .rollup(dataByGroup => {
  //       const aggregatedData = getAggregationData(dataByGroup, select);
  //       return getDataByFieldName([dataByGroup[0]], fieldWithoutAggregation, aggregatedData,[]);
  //     })
  //     .entries(rawData);

  //   result = result.map(data => {
  //     return data.value[0];
  //   });
  // } else {
  //   // CBT:if group by single field
  //   result = d3
  //     .nest()
  //     .key(keyData => {
  //       return keyData[groupby.field];
  //     })
  //     .rollup(dataByGroup => {
  //       const aggregatedData = getAggregationData(dataByGroup, select);
  //       return getDataByFieldName([dataByGroup[0]], fieldWithoutAggregation, aggregatedData,[]);
  //     })
  //     .entries(rawData);

  //   result = result.map(data => {
  //     return data.value[0];
  //   });
  // }
  // return result;
}

// function getAggregationData(dataByGroup, select) {
//   const aggregationToD3ArrayMaiing = {
//     avg: { alias: "mean" }
//   };
//   const aggregationToD3Maiing = {
//     distinct: { alias: "set", cbFunction: "values" }
//   };
//   //CBT:get all fiels from select which have aggregation key
//   const aggregationFields = select.filter(data => data.hasOwnProperty("aggregation"));
//   let aggregatedDataForAllFields = {};
//   aggregationFields.forEach(aggregationFieldData => {
//     function performaAggregation(dataByGroup, aggregation, fieldKey) {
//       const aggregatedData = {};
//       //CBT:if aggregation contains string
//       if (aggregation == "count") {
//         aggregatedData[aggregationFieldData.alias || aggregation + "(" + fieldKey + ")"] = dataByGroup.map(data => {
//           return aggregationFieldData.table!=null ?  data[aggregationFieldData.table+"."+fieldKey] : data[fieldKey];
//         }).length;
//       } else if (d3Array[aggregation]) {
//         //CBT:if aggregation available in d3-array
//         aggregatedData[aggregationFieldData.alias || aggregation + "(" + fieldKey + ")"] = d3Array[aggregation](dataByGroup, data => {
//           return Math.abs(aggregationFieldData.table!=null ? data[aggregationFieldData.table+"."+fieldKey] :data[fieldKey]);
//         });
//       } else if (aggregationToD3ArrayMaiing[aggregation]) {
//         //CBT:if aggregation available in aggregation to d3-array mapping
//         aggregatedData[aggregationFieldData.alias || aggregation + "(" + fieldKey + ")"] = d3Array[aggregationToD3ArrayMaiing[aggregation].alias](dataByGroup, data => {
//           return Math.abs(aggregationFieldData.table!=null ? data[aggregationFieldData.table+"."+fieldKey] :data[fieldKey]);
//         });
//       } else if (aggregationToD3Maiing[aggregation]) {
//         //CBT:if aggregation available in aggregation to d3 mapping
//         aggregatedData[aggregationFieldData.alias || aggregation + "(" + fieldKey + ")"] = d3[aggregationToD3Maiing[aggregation].alias](dataByGroup, data => {
//           return aggregationFieldData.table!=null ? data[aggregationFieldData.table+"."+fieldKey] :data[fieldKey];
//         });
//         if (aggregationToD3Maiing[aggregation].cbFunction) {
//           aggregatedData[aggregationFieldData.alias || aggregation + "(" + fieldKey + ")"] = aggregatedData[aggregationFieldData.alias || aggregation + "(" + fieldKey + ")"][aggregationToD3Maiing[aggregation].cbFunction]();
//         }
//       }
//       return aggregatedData;
//     }

//     let aggregation = null;
//     if (Array.isArray(aggregationFieldData.aggregation)) {
//       let aggregationArray = [...aggregationFieldData.aggregation];
//       //CBT: Step 1 start
//       aggregation = aggregationArray[aggregationArray.length - 1];
//       let aggregatedDataResult = performaAggregation(dataByGroup, aggregation, aggregationFieldData.field);
//       //CBT: Step 1 end
//       //CBT:if aggregation contains array then performa aggregation operation from n-2 to 0 elements
//       //Example ['count',distinct'] then 'distinct ' calculated using step 1 now percont only count on output data of distinct value
//       aggregationArray.pop(); //Remove last element ex. distinct
//       let fieldForAggregation = Object.keys(aggregatedDataResult)[0];
//       let dateForAggregation = aggregatedDataResult[fieldForAggregation].map(data => {
//         return { [fieldForAggregation]: data };
//       });
//       aggregationArray.reverse();
//       aggregationArray.forEach(aggregationValue => {
//         aggregatedDataResult = performaAggregation(dateForAggregation, aggregationValue, fieldForAggregation);
//         fieldForAggregation = Object.keys(aggregatedDataResult)[0];
//         if (Array.isArray(aggregatedDataResult[fieldForAggregation])) {
//           dateForAggregation = aggregatedDataResult[fieldForAggregation].map(data => {
//             return { [fieldForAggregation]: data };
//           });
//         } else {
//           dateForAggregation = [{ [fieldForAggregation]: aggregatedDataResult[fieldForAggregation] }];
//         }
//       });
//       aggregatedDataForAllFields = { ...aggregatedDataForAllFields, ...aggregatedDataResult };
//     } else {
//       aggregation = aggregationFieldData.aggregation;
//       aggregatedDataForAllFields = { ...aggregatedDataForAllFields, ...performaAggregation(dataByGroup, aggregation, aggregationFieldData.field) };
//     }
//   });
//   return aggregatedDataForAllFields;
// }

// function getDataByFieldName(dataByGroup, select, aggregatedData,tableAlias) {
//   //CBT:select all fields from select which doesn't have aggregation key
//   let returnData = [];
//   returnData = dataByGroup.map(data => {
//     const dataObj = { ...aggregatedData };
//     select.map(column => {
//       if(column.table==null && tableAlias.length>0){
//         //CBT:check if both table have same number of columns
//         let isSameColumnInTables=false;
//         const sameColumns=[];
//         for(i=0;i<tableAlias.length;i++){
//           let tableColumn=tableAlias[i]+"."+column.field;
//           for(j=i+1;j<tableAlias.length;j++){
//             let columTableName=tableAlias[j]+"."+column.field;
//             if(data.hasOwnProperty(tableColumn)==true &&  data.hasOwnProperty(columTableName)==true){
//               isSameColumnInTables=true;
//               sameColumns.push(column.field);
//             }
//           }
//         }
//        if(isSameColumnInTables==true){
//          throw new Error("Select Data Ambiguous columns:"+sameColumns.join(","));
//        } 
//       }
//       dataObj[column.alias || column.field] = column.table!=null ? data[column.table+"."+column.field] :data[column.field];
//     });
//     return dataObj;
//   });
//   return returnData;
// }

function processJoinCondition(tableData,typeOfJoin,joincondition){
  const leftMostTableDataObj={};
  const rightMostTableDataObj={};
  const finalReult=[];
  const tableAliasData={};
  let joinConditionResult=createJoinCondition(joincondition,{},tableAliasData,true);
  let filterCondition={
    joincondition:joinConditionResult.joincondition,
    leftTable:tableAliasData.leftTable,
    rightTable:tableAliasData.rightTable
  };
  
  const columnsLeftMosTables=tableData[filterCondition.leftTable].length>0 ? Object.keys(tableData[filterCondition.leftTable][0]) :[];
  const columnsRightMosTables=tableData[filterCondition.rightTable].length>0 ? Object.keys(tableData[filterCondition.rightTable][0]) :[];
  if(typeOfJoin=='left' || typeOfJoin=='inner'){
      tableData[filterCondition.leftTable].forEach(d1=>{
          joinConditionResult=createJoinCondition(joincondition,d1,tableAliasData,false);
          filterCondition={
            joincondition:joinConditionResult.joincondition,
            leftTable:tableAliasData.leftTable,
            rightTable:tableAliasData.rightTable
          };
          const filteredData2 = tableData[filterCondition.rightTable].filter(row => {
            return processFilter(joincondition, row,[]);
          });

          if(typeOfJoin === 'inner' && filteredData2.length === 0){
            //CBT: do nothing
          } else if(filteredData2.length === 0){
            const leftTableData={};
            columnsLeftMosTables.forEach(column=>{
              leftTableData[filterCondition.leftTable+"."+column]=d1[column];
            });
            columnsRightMosTables.forEach(column=>{
              leftTableData[filterCondition.rightTable+"."+column]=null;
            })
            finalReult.push(leftTableData);
          }else {
            const leftTableData={};
            columnsLeftMosTables.forEach(column=>{
              leftTableData[filterCondition.leftTable+"."+column]=d1[column];
            });
            filteredData2.forEach(data2=>{
              const obj = {...leftTableData};
              columnsRightMosTables.forEach(column=>{
                obj[filterCondition.rightTable+"."+column]=data2[column];
              })
              finalReult.push(obj);
            })
          }
      });
  }else{
    //CBT: 'right' join
    tableData[filterCondition.rightTable].forEach(d1=>{
      joinConditionResult=createJoinCondition(joincondition,d1,tableAliasData,false);
      filterCondition={
        joincondition:joinConditionResult.joincondition,
        leftTable:tableAliasData.leftTable,
        rightTable:tableAliasData.rightTable
      };
        const filteredData2 = tableData[filterCondition.leftTable].filter(row => {
          return processFilter(joincondition, row,[]);
        });

        if(filteredData2.length === 0){
          const rightTableData={};
          columnsRightMosTables.forEach(column=>{
            rightTableData[filterCondition.rightTable+"."+column]=d1[column];
          });
          columnsLeftMosTables.forEach(column=>{
            rightTableData[filterCondition.leftTable+"."+column]=null;
          })
          finalReult.push(rightTableData);
        }else {
          const rightTableData={};
          columnsRightMosTables.forEach(column=>{
            rightTableData[filterCondition.rightTable+"."+column]=d1[column];
          });
          filteredData2.forEach(data2=>{
            const obj = {...rightTableData};
            columnsLeftMosTables.forEach(column=>{
              obj[filterCondition.leftTable+"."+column]=data2[column];
            })
            finalReult.push(obj);
          })
        }
    });
  } 
  return finalReult;
}

function createJoinCondition(joincondition,value,tableAliasData,shouldReplaceField){
  if (joincondition.AND || joincondition.and) {
    processJoinAND(joincondition.AND || joincondition.and, value,tableAliasData,shouldReplaceField);
  } else if (joincondition.OR || joincondition.or) {
    processJoinOR(joincondition.OR || joincondition.or, value,tableAliasData,shouldReplaceField);
  } else if (joincondition.NONE) {
    const result=createSingleJoinCondition(joincondition.NONE, value,tableAliasData,shouldReplaceField);
  } else {
    const result=createSingleJoinCondition(joincondition, value,tableAliasData,shouldReplaceField);
  }
  return {joincondition:joincondition};
}

function createSingleJoinCondition(filter,value,tableAliasData,shouldReplaceField){
  filter.field=(shouldReplaceField==true || shouldReplaceField==null) ? filter.value.field :filter.field;
  if(tableAliasData.leftTable==null){
    const leftTable=filter.table;
    const rightTable=filter.value.table;
    tableAliasData.leftTable=tableAliasData.leftTable!=null ?tableAliasData.leftTable : leftTable;
    tableAliasData.rightTable=tableAliasData.rightTable!=null ? tableAliasData.rightTable :rightTable;
  }
  filter.value=value[filter.field];
  if(filter.table)
    delete filter.table;
}

function processJoinAND(arrANDFilters,value,tableAliasData,shouldReplaceField){
  arrANDFilters.forEach(f => {
     createJoinCondition(f,value,tableAliasData,shouldReplaceField);
  });
}
function processJoinOR(arrANDFilters,value,tableAliasData,shouldReplaceField){
  arrANDFilters.forEach(f => {
     createJoinCondition(f,value,tableAliasData,shouldReplaceField);
  });
}


function processFilter(filter, row,tableAlias) {
  if (filter.AND || filter.and) {
    return processAND(filter.AND || filter.and, row,tableAlias);
  } else if (filter.OR || filter.or) {
    return processOR(filter.OR || filter.or, row,tableAlias);
  } else if (filter.NONE) {
    return createSingleCondition(filter.NONE, row,tableAlias);
  } else {
    return createSingleCondition(filter, row,tableAlias);
  }
}
function processAND(arrANDFilters, row,tableAlias) {
  return arrANDFilters.every(f => {
    return processFilter(f, row,tableAlias);
  });
}
function processOR(arrANDFilters, row,tableAlias) {
  return arrANDFilters.some(f => {
    return processFilter(f, row,tableAlias);
  });
}

function createSingleCondition(filter, rawData,tableAlias) {
  let field = filter.field,
    table = filter.table ? (filter.table+".") : "",
    aggregation = filter.aggregation ? filter.aggregation : null,
    operator = filter.operator,
    value = filter.value,
    encloseField=filter.encloseField;
  checkEncloseField(encloseField);
  let filteredData = [];
  let conditiontext = "";
  if (aggregation != null) {
    //CBT:need to implement
    // if (Object.prototype.toString.call(aggregation).toLowerCase() === "[object array]") {
    //   let aggregationText = "";
    //   aggregation.forEach(function(d) {
    //     aggregationText = aggregationText + d + "("
    //   });
    //   conditiontext = aggregationText + field;
    //   aggregationText = "";
    //   aggregation.forEach(function(d) {
    //     aggregationText = aggregationText + ")"
    //   });
    //   conditiontext = conditiontext + aggregationText;
    // } else {
    //   conditiontext = aggregation + '(' + field + ')';
    // }
  } else {
    conditiontext = table+field;
  }
  if(filter.table==null && tableAlias.length>0){
    //CBT:check if both table have same number of columns
    let isSameColumnInTables=false;
    const sameColumns=[];
    for(i=0;i<tableAlias.length;i++){
      let tableColumn=tableAlias[i]+"."+field;
      for(j=i+1;j<tableAlias.length;j++){
        let columTableName=tableAlias[j]+"."+field;
        if(rawData.hasOwnProperty(tableColumn)==true &&  rawData.hasOwnProperty(columTableName)==true){
          isSameColumnInTables=true;
          sameColumns.push(field);
        }
      }
    }
   if(isSameColumnInTables==true){
     throw new Error("Filter Data Ambiguous columns:"+sameColumns.join(","));
   } 
  }

  if (operator != undefined) {
    let sign = operatorSign(operator, value);
    if(typeof(rawData[conditiontext])=='string')
       rawData[conditiontext]=rawData[conditiontext].toLowerCase();
    if(typeof(value)=='string')
      value=value.toLowerCase();
    if(Array.isArray(value)==true){
      for(let i=0;i<value.length;i++){
        if(typeof(value[i])=='string'){
          value[i]=value[i].toLowerCase();
        }
      }
    }
    filteredData = operatorrToFunation[sign](rawData[conditiontext],value);
  }
  return filteredData;
}

function operatorSign(operator, value) {
  let sign = "";
  if (operator.toString().toLowerCase() == "eq") {
    if (Object.prototype.toString.call(value) === "[object Array]") {
      sign = "indexOf";
    } else if (typeof value === "undefined" || value == null) {
      sign = "==";
    } else if (typeof value == "string") {
      sign = "==";
    } else {
      sign = "==";
    }
  } else if (operator.toString().toLowerCase() == "noteq") {
    if (Object.prototype.toString.call(value) === "[object Array]") {
      sign = "!indexOf";
    } else if (typeof value === "undefined" || value == null) {
      sign = "!=";
    } else if (typeof value == "string") {
      sign = "!=";
    } else {
      sign = "!=";
    }
  } else if (operator.toString().toLowerCase() == "match") {
    sign = "==";
  } else if (operator.toString().toLowerCase() == "notmatch") {
    sign = "!=";
  } else if (operator.toString().toLowerCase() == "gt") {
    sign = ">";
  } else if (operator.toString().toLowerCase() == "lt") {
    sign = "<";
  } else if (operator.toString().toLowerCase() == "gteq") {
    sign = ">=";
  } else if (operator.toString().toLowerCase() == "lteq") {
    sign = "<=";
  } else {
    throw Error("Unknow operator '%s'", operator);
  }
  return sign;
}

function executeQueryStream(connection, query, onResultFunction, cb) {
  cb({
    status: false,
    error:"Need to be implement"
  });
}
function checkEncloseField(encloseField){
  if(encloseField!=null){
    throw new Error("encloseField is not supported now");
  }
}

module.exports = {
  executeQuery: executeQuery,
  executeQueryStream: executeQueryStream
};
