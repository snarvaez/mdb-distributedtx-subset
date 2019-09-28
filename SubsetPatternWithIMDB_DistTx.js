// ON 4.2 SHARDED ONLY
/*
// 1-TIME SETUP
// ==============================================
use IMDB
db.dropDatabase()
sh.enableSharding("IMDB");
sh.shardCollection("IMDB.movies_metadata", { _id: 1 } );
sh.shardCollection("IMDB.movies_credits", { _id: 1 } );
db.adminCommand({ movePrimary: "IMDB", to: "sh_0" });

use IMDB_Cast
db.dropDatabase()
sh.enableSharding("IMDB_Cast");
sh.shardCollection("IMDB_Cast.cast_crew", { _id: 1 } );
db.adminCommand({ movePrimary: "IMDB_Cast", to: "sh_1" });

sh.status();
exit

mongoimport --host YOURHOST --ssl --username YOURUSER --password YOURPWD --authenticationDatabase admin --db IMDB --collection movies_credits --type JSON --file IMDB__movies_credits.json
mongoimport --host YOURHOST --ssl --username YOURUSER --password YOURPWD --authenticationDatabase admin --db IMDB --collection movies_metadata --type JSON --file IMDB__movies_metadata.json
mongoimport --host YOURHOST --ssl --username YOURUSER --password YOURPWD --authenticationDatabase admin --db IMDB_Cast --collection cast_crew --type JSON --file IMDB_Cast__cast_crew.json

// Connect to mongo using the shell and demonstrate that each DB lives in a different shard
sh.status();
exit

// ==============================================
*/

// DEMO
// ==============================================
// REMOVE SUBSET ARRAYS
// RUN THIS EVERY TIME TO CLEAN-UP PREVIOUS RUNS
use IMDB_Cast
var bulk = db.cast_crew.initializeUnorderedBulkOp();
bulk.find({}).update({$unset: {"roles": ""}});
bulk.execute();

use IMDB
var bulk = db.movies_metadata.initializeUnorderedBulkOp();
bulk.find({}).update({$unset: {"main_actors": ""}});
bulk.execute();
print("=== DONE UNSETS ===");

// SET SUBSET ARRAYS WITH TRANSACTIONS FOR 100 MOVIES
use IMDB

db.movies_credits.find().sort({_id:1}).limit(100).forEach(function(credit) {
    
    var s = db.getMongo().startSession();
    s.startTransaction({readConcern: {level: 'snapshot'}, writeConcern: {w: 1}});

    var movieColl = s.getDatabase('IMDB').getCollection('movies_metadata');
    var castColl = s.getDatabase('IMDB_Cast').getCollection('cast_crew');
    var movie = movieColl.findOne({_id: credit._id});
    var main_actors = [];

    print("[" + movie._id + "] " + movie.title);
    
    credit.cast.forEach(function(credit_cast) {

        main_actors.push({
            "name": credit_cast.name,
            "character": credit_cast.character,
            "cast_id": credit_cast.id
        });

        castColl.update(
            {_id: credit_cast.id},
            {$push: {"roles": 
                {
                    "_id": movie._id,
                    "title": movie.title,
                    "release_date": movie.release_date,
                    "character": credit_cast.character
                }
            }
        });
    });

    movieColl.update(
        {_id: credit._id},
        {$set: { "main_actors": main_actors }
    });

    s.commitTransaction();
});

print("=== DONE  ===");
