const   Promise     = require('bluebird'),
        MongoClient = require('mongodb').MongoClient,
        ObjectId 	= require('mongodb').ObjectId,
        baby        = require('babyparse');

const   dbConnect       	= Promise.promisify(MongoClient.connect);
		
const	hostname			= 'mongodb://127.0.0.1:27017',
		dbName				= 'squad-server2-1', //'wargaming', //,
		userCollectionName	= 'users',
		schoolID			= '57b6c9a6dd69264b6c5ba82d';


const 	fileCsvName 		= 'aa4aa1f5-720f-5a4d-b386-b3b74ba7fee8.csv';


let dbReference;

dbConnect(`${hostname}/${dbName}`).then(db => {
	dbReference = db;
	const userCollection = db.collection(userCollectionName);
	const schoolCollection = db.collection('schools');

	return parse(fileCsvName).then(userArr => {
		userArr.forEach(userObj => {
			const formCursor = schoolCollection.find({"_id": ObjectId(schoolID)}, {"forms": {"$elemMatch": {"name": userObj.Form}}, "_id": 0, "forms._id":true });
			const houseCursor = schoolCollection.find({"_id": ObjectId(schoolID)}, {"houses": {"$elemMatch": {"name": userObj.House}}, "_id": 0, "houses._id":true });
			
			let formId;
			let houseId;	

			/** get form id */
			formCursor.toArray().then(formData => {
				
				formData.forEach(el => {
					formId = el.forms[0]._id.toString();
				});

				/** get house id */
				houseCursor.toArray().then(houseData => {
					houseData.forEach(el => {
						houseId = el.houses[0]._id.toString();
					});

					/** update student */
					userCollection.update( {"_id": ObjectId(userObj.id) }, { $set: {"firstName": userObj.firstName, "lastName": userObj.lastName, "gender": userObj.gender, 
							"permissions.0.details.formId": ObjectId(formId), "permissions.0.details.houseId": ObjectId(houseId)  } });	
					
					//userCollection.update( {"_id": ObjectId(userObj.id) }, { $set: {"firstName": userObj.firstName, "lastName": userObj.lastName, "gender": userObj.gender } });
					//userCollection.insert({"_id": ObjectId(userObj.id), "firstName": userObj.firstName, "lastName": userObj.lastName, "gender": userObj.gender, "age": "35" });

				});			
			});
		});
	});

}).then( () => {
	setTimeout(function(){
		dbReference.close();	
	}, 5000);
});


function parse (file) {
    return getPromiseFromCSVFile(file).then(result => {
        return result.data || [];
    });
}

function getPromiseFromCSVFile (file) {
    return new Promise((resolve, reject) => {
        const parsedResponse = baby.parseFiles(file, {
            header:     true,
            complete:	(results, file) => {
                /** throw Error for reading invalid file */
                if (results.errors.length > 0) {
                    reject(new Error(results.errors[0].message));
                } else {
                    resolve(results)
                }
            }
        });
        /** throw Error if file is unsupported */
        if (parsedResponse.errors.length > 0) {
            reject(new Error(parsedResponse.errors[0].message));
        }
        // no return
    });
}