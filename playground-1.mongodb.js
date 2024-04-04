// Ava Mo

db.autos.insertMany( [
    { model: "Accord",
       make: "Honda",
       type: "Sedan",
       instock: [ { store: "Fraser", qty: 20 }, { store: "Maple", qty: 25 }, { store: "Moody", qty: 20 } ]
    },
    { model: "Camry",
       make: "Toyota",
       type: "Sedan",
       instock: [ { store: "Moody", qty: 30 }, { store: "North Shore", qty: 40 }, { store: "Rocky", qty: 45 } ]
    },
    { model: "CRV",
       make: "Honda",
       type: "SUV",
       instock: [ { store: "Fraser", qty: 20 }, { store: "North Shore", qty: 30 }, { store: "Rocky", qty: 35 }, { store: "Valley", qty: 30 } ]
    },
    { model: "Rav4",
       make: "Toyota",
       type: "SUV",
       instock: [ { store: "Fraser", qty: 40 }, { store: "Moody", qty: 30 } ]
    },
    { model: "Edge",
       make: "Ford",
       type: "SUV",  
       instock: [ { store: "Fraser", qty: 15 },  { store: "Moody", qty: 15 }, { store: "Rocky", qty: 25 }, { store: "Valley", qty: 20 } ]
    },
    { model: "Ridgeline",
       make: "Honda",
       type: "Truck",
       instock: [ { store: "Maple", qty: 35 }, { store: "Rocky", qty: 20 }, { store: "Valley", qty: 15 } ]
    },
    { model: "F-150",
       make: "Ford",
       type: "Truck",
       instock: [ { store: "Fraser", qty: 35 }, { store: "North Shore", qty: 35 } ]
    }
  ] );
  
// 1.Find the SUV or Truck products that are available in more than two stores. Display the model and type fields, 
//as well as the total qty of all available stores, and sort the result in ascending order of the total qty.

db.autos.aggregate([
    { 
      $match: { 
        $or: [
          { type: "SUV" },
          { type: "Truck" }
        ] 
      } 
    },
    {
      $match: {
        "instock.2":{$exists:true}
      }
    },
    {
        $unwind:"$instock"
    },
    {
        $group: {
          _id:{model:"$model",type:"$type"},
          toatl_qty: {
            $sum: "$instock.qty"
          }
        }

    },
    {
        $project: {
          _id:0,
          model:"$_id.model",
          type:"$_id.type",
          toatl_qty:"$toatl_qty"
        }
    },
    {$sort: {
      toatl_qty: 1
    }}
  ]);

  //2.Find each store that has at least 50 SUV products. 
  //Display the store name and the total qty of SUVs in the store and sort the result in descending order of the total qty. 
  //The result is given below for your reference. The field names and the order of the fields in your result must be the same.

  db.autos.aggregate([
    {
      $unwind: "$instock"
    },
    {
      $match: {
        type: "SUV"
      }
    },
    {
      $group: {
        _id: "$instock.store",
        total_qty: { $sum: "$instock.qty" }
      }
    },
    {
      $match: {
        total_qty:{$gte:50}
      }
    },
    {
      $sort: { total_qty: -1 }
    },
    {
      $project: {
        _id: 0,
        store: "$_id",
        total_qty:"$total_qty"
      }
    }
  ])

  // 3.Find the models with the second lowest total qty in all the stores. 
  //The result is given below for your reference. The field names and order of the fields in your result must be the same; the order of the array elements does not matter.

  db.autos.aggregate([
    { $unwind: "$instock" },
    { $group: { _id: "$model", total_qty: { $sum: "$instock.qty" } } },
    { $sort: { total_qty: 1 } },
    { $group: { _id: "$total_qty", models: { $push: "$_id" } } },
    { $sort: { _id: 1 } },
    { $skip: 1 },
    { $limit: 1 },
    { $project: { _id: 0, models: 1, total_qty: "$_id" } }
  ])

  //Find the products that have a quantity greater than 30 in at least two stores 
  //(e.g., Camry and F-150). Display the model, make, and instock fields in the result, 
  //where the instock array only consists of the stores with a quantity greater than 30. 
  //The result is given below for your reference. The field names and the order of the fields in your result must be the same; 
  //the order of the documents and the order of the array elements do not matter.

  db.autos.aggregate([
    { $unwind: "$instock" },
    { $match: { "instock.qty": { $gt: 30 } } },
    {
      $group: {
        _id: { model: "$model", make: "$make" },
        instock: { $push: "$instock" }
      }
    },
    { $match: { "instock.1": { $exists: true } } },
    {
        $project: {
          model:"$_id.model",
          make:"$_id.make",
          _id:0,
          instock:"$instock"
        }
    }
  ]);

