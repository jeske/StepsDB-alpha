
// BsonHelper - a thread-safe bi-directional skiplist
//
// authored by David W. Jeske (2008-2010)
//
// This code is provided without warranty to the public domain. You may use it for any
// purpose without restriction.

using System;
using MongoDB.Bson;

namespace Bend {
    public class BsonHelper {

        #region Main applyUpdateCommands() Entry Point

        public static void applyUpdateCommands(BsonDocument doc, BsonDocument change_spec) {
            foreach (var field in change_spec) {
                if (field.Name.Length > 0 && field.Name[0] == '$') {
                    // update command
                    switch (field.Name) {
                        case "$set":
                            _applySet(doc, field.Value.AsBsonDocument);
                            break;
                        case "$inc":
                            _applyInc(doc, field.Value.AsBsonDocument);
                            break;
                        case "$unset":
                            if (doc.Contains(field.Name)) {
                                doc.Remove(field.Name);
                            }
                            break;
                        case "$push":
                        case "$pushAll":
                        case "$addToSet":
                        case "$pop":
                        case "$pull":
                        case "$pullAll":
                        case "$rename":
                        case "$":
                            throw new Exception("unimplemented update operator: " + field.Name);
                        default:
                            throw new Exception("unknown update operator: " + field.Name);
                    }

                } else {
                    // field replace
                    if (field.Value.BsonType == BsonType.Document) {
                        if (!doc.Contains(field.Name) ||
                            doc.GetElement(field.Name).Value.BsonType != BsonType.Document) {
                            // make a document to hold the recurse                        
                            doc.Set(field.Name, new BsonDocument());
                        }
                        // recursively apply changes                            
                        applyUpdateCommands(doc.GetElement(field.Name).Value.AsBsonDocument,
                            field.Value.AsBsonDocument);
                    } else {
                        // otherwise just apply the change directly
                        doc.Set(field.Name, field.Value);
                    }
                }
            }
        }

        #endregion

        #region Private Apply Implementations

        private static void _applySet(BsonDocument doc, BsonDocument changes) {
            foreach (var field in changes) {
                doc.Set(field.Name, field.Value);
            }
        }
        

        private static void _applyInc(BsonDocument doc, BsonDocument changes) {
            foreach (var field in changes) {
                if (!field.Value.IsNumeric) {
                    throw new Exception(String.Format("update $inc field not numeric: {0}:{1}",
                        field.Name, field.Value.ToJson()));
                }
                if (doc.Contains(field.Name)) {                       
                    var element = doc.GetElement(field.Name);
                    if (element.Value.IsNumeric) {
                        switch (element.Value.BsonType) {
                            case BsonType.Int64:                                
                                element.Value = element.Value.AsInt64 + field.Value.AsInt64;
                                break;
                            case BsonType.Int32:
                                element.Value = element.Value.AsInt32 + field.Value.AsInt32;
                                break;
                            case BsonType.Double:
                                element.Value = element.Value.AsDouble + field.Value.AsDouble;
                                break;

                            // ?? case BsonType.Timestamp:
                            default:
                                throw new Exception("unknown numeric type: " + element.Value.BsonType);
                        }
                        continue; // goto the next increment int he loop
                    }
                }
                // we couldn't increment, so set! 
                doc.Set(field.Name, field.Value);
            }
        }

        #endregion
    }
}