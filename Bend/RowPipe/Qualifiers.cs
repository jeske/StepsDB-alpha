// Copyright (C) 2008, by David W. Jeske
// All Rights Reserved.


using System;
using System.Collections.Generic;

namespace Bend
{
    // ----------------------------------[   qualifiers ]-------------------------------

    public enum QualifierResult
    {
        DESIRE_LT,    // we desire a key less than the current key
        DESIRE_GT,    // we desire a key greater than the current key
        MATCH,        // the current key matches
        NOMATCH       // the current key does not match, and this operator is nor orderable
    }

    public enum QualifierSetupResult
    {
        SETUP_OK,           // we are setup to watch for the next key in a scan
        NO_MORE_MATCHES     // there are no more matching keys in this direction
    }

    public abstract class QualifierBase
    {
        public abstract QualifierResult KeyCompare(string key);
        public abstract QualifierSetupResult setupForNext(string key); // return false if the next key is invalid
        public abstract QualifierSetupResult setupForPrev(string key); // return false if the prev key is invalid
        // TODO: how do we standardize the encoding across equals, gt, gt, contains, etc?
        //   maybe these should be all part of a "datatype" implementation,
        //   instead of spread out in multiple classes
    }

    public class QualifierException : Exception
    {
        public QualifierException(string msg) : base(msg) { }
    }


    public sealed class QualifierAny : QualifierBase
    {
        public QualifierAny() {
        }
        public override string ToString() {
            return "*";
        }
        public override QualifierResult KeyCompare(string keydata) {
            return QualifierResult.MATCH;
        }
        public override QualifierSetupResult setupForNext(string key) {
            return QualifierSetupResult.SETUP_OK;
        }
        public override QualifierSetupResult setupForPrev(string key) {
            return QualifierSetupResult.SETUP_OK;
        }
    }

    public sealed class QualifierExact : QualifierBase
    {
        string value;
        int exact_hash_delta;
        public QualifierExact(string value) {
            this.value = value;
            if (value == null) {
                throw new QualifierException("QualifierExact may not be null");
            }
            exact_hash_delta = "EXACT".GetHashCode();
        }
        public override string ToString() {
            return "=" + value;
        }
        public override QualifierResult KeyCompare(string keydata) {
            if (keydata == null) {
                throw new QualifierException("QualifierExact.KeyCompare(null) is invalid");
            }
            int compare_result = value.CompareTo(keydata);
            if (compare_result == 0) {  // equals
                return QualifierResult.MATCH;
            } else if (compare_result < 0) { //  QUAL_TARGET < keydata
                return QualifierResult.DESIRE_LT;
            } else {  // QUAL_TARGET > keydata
                return QualifierResult.DESIRE_GT;
            }
        }
        public override QualifierSetupResult setupForNext(string keydata) {
            if (keydata == null) {
                throw new QualifierException("QualifierExact.setupForNext(null) is invalid");
            }
            if (this.value == keydata) {
                return QualifierSetupResult.SETUP_OK;
            } else {
                return QualifierSetupResult.NO_MORE_MATCHES;
            }
        }
        public override QualifierSetupResult setupForPrev(string keydata) {
            if (keydata == null) {
                throw new QualifierException("QualifierExact.setupForPrev(null) is invalid");
            }
            if (this.value == keydata) {
                return QualifierSetupResult.SETUP_OK;
            } else {
                return QualifierSetupResult.NO_MORE_MATCHES;
            }
        }
        public override bool Equals(object obj) {
            if (obj.GetType() != typeof(QualifierExact)) {
                return false;
            } else {
                QualifierExact obj_exact = (QualifierExact)obj;
                return this.value.Equals(obj_exact.value);
            }
        }
        public override int GetHashCode() {
            return this.exact_hash_delta + value.GetHashCode();
        }
    }

}