// Copyright (C) 2008, by David W. Jeske
// All Rights Reserved.


using System;
using System.Collections.Generic;

namespace Bend
{
    // ----------------------------------[   qualifiers ]-------------------------------
    public interface IScanner<K>
    {
        bool MatchTo(K candidate);
        IComparable<K> genLowestKeyTest();
        IComparable<K> genHighestKeyTest();
    }

    public abstract class QualifierBase : IScanner<string>
    {
        public abstract bool MatchTo(string candidate);
        public abstract IComparable<string> genLowestKeyTest();
        public abstract IComparable<string> genHighestKeyTest();
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

        public override IComparable<string> genLowestKeyTest() {
            return "<";
        }
        public override IComparable<string> genHighestKeyTest() {
            return ">";
        }
        public override bool MatchTo(string keydata) {
            return true;
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
        public override bool MatchTo(string key) {
            return this.ToString().Equals(key);
        }
        public override IComparable<string> genLowestKeyTest() {
            return "=" + value;
        }
        public override IComparable<string> genHighestKeyTest() {
            return "=" + value;
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