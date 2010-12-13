
// BsonHelper - an evaluation engine for MongoDB style update commands
//
// authored by David W. Jeske (2008-2010)
//
// This code is provided without warranty to the public domain. You may use it for any
// purpose without restriction.

//
//
//
//   http://www.mongodb.org/display/DOCS/Updating
//
//

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
                            _applyUnset(doc, field.Value.AsBsonDocument);
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

        private static void _traverseToField(BsonDocument doc, string field_name, out string last_field_part, out BsonDocument subdoc) {
            string[] parts = field_name.Split('.');
            BsonDocument doc_cur = doc;

            for (int pos = 0; pos < (parts.Length - 1); pos++) {
                var part_name = parts[pos];
                if (doc_cur.Contains(part_name) &&
                    doc_cur.GetElement(part_name).Value.BsonType == BsonType.Document) {
                    doc_cur = doc_cur.GetElement(part_name).Value.AsBsonDocument;
                } else {
                    var newsubdoc = new BsonDocument();
                    doc_cur.Set(part_name, newsubdoc);
                    doc_cur = newsubdoc;
                }
            }
           
            last_field_part = parts[parts.Length - 1];
            subdoc = doc_cur;

        }

        private static void _applyInc(BsonDocument topdoc, BsonDocument changes) {
            foreach (var field in changes) {
                string field_name;
                BsonDocument doc;
                _traverseToField(topdoc, field.Name, out field_name, out doc);

                if (!field.Value.IsNumeric) {
                    throw new Exception(String.Format("update $inc field not numeric: {0}:{1}",
                        field_name, field.Value.ToJson()));
                }
                if (doc.Contains(field_name)) {                       
                    var element = doc.GetElement(field_name);
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
                doc.Set(field_name, field.Value);
            }
        }



        private static void _applyUnset(BsonDocument topdoc, BsonDocument changes) {
            // TODO: this is going to create the entire document path going down
            //   to the unset, but that's NOT what what we should do.

            foreach (var field in changes) {
                string field_name;
                BsonDocument doc;
                _traverseToField(topdoc, field.Name, out field_name, out doc);

                if (doc.Contains(field_name)) {
                    doc.Remove(field_name);
                }
            }
            

        }

        #endregion
    }
}