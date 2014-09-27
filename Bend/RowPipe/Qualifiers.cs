// Copyright (C) 2008-2014 David W. Jeske
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License. See the AUTHORS file
// for names of contributors.


using System;
using System.Collections.Generic;

namespace Bend
{
    // ----------------------------------[   qualifiers ]-------------------------------
   
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