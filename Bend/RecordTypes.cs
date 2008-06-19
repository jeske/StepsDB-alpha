// Copyright (C) 2008, by David W. Jeske
// All Rights Reserved.


using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Bend
{

// ---------------[ Record* ]---------------------------------------------------------

public enum RecordDataState
{
    NOT_PROVIDED,
    FULL,
    INCOMPLETE
}
public enum RecordUpdateResult
{
    SUCCESS,
    FINAL

}
class RecordData
{
    RecordKey key;
    RecordDataState state;
    String data;
    public RecordData(RecordDataState initialState, RecordKey key, String data)
    {
        this.key = key;
        this.state = initialState;
        this.data = data;
    }
    public RecordData(RecordDataState initialState, RecordKey key) :
        this(initialState, key, null) { }

    public RecordUpdateResult applyUpdate(RecordUpdate update)
    {
        switch (update.type)
        {
            case RecordUpdateTypes.DELETION_TOMBSTONE:
                return RecordUpdateResult.FINAL;
            case RecordUpdateTypes.FULL:
                this.state = RecordDataState.FULL;
                this.data = update.data;
                return RecordUpdateResult.FINAL;
            case RecordUpdateTypes.NONE:
                return RecordUpdateResult.SUCCESS;
            case RecordUpdateTypes.PARTIAL:
                throw new Exception("partial update not implemented");
            default:
                throw new Exception("unknown update type");

        }
    }

    public override String ToString()
    {
        return "RD(" + this.data + ")";
    }
}

public enum RecordUpdateTypes
{
    DELETION_TOMBSTONE,
    PARTIAL,
    FULL,
    NONE
}
public class RecordUpdate
{
    public RecordUpdateTypes type;
    public String data;
    public RecordUpdate(RecordUpdateTypes type, String data)
    {
        this.type = type;
        this.data = data;
    }

    public override String ToString()
    {
        return this.data;
    }


    public String DebugToString() {
        return "RU(" + this.data + ")";
    }

    public byte[] encode() {
        System.Text.ASCIIEncoding enc = new System.Text.ASCIIEncoding();
        byte[] data = enc.GetBytes(this.data);
        return data;
    }

}


public class RecordKey : IComparable<RecordKey>
{
    List<String> key_parts;
    
    public RecordKey()
    {
        key_parts = new List<String>();
    }
    public RecordKey(byte[] data)
        : this() {
        decode(data);
    }


    public void appendKeyPart(String part)
    {
        key_parts.Add(part);
    }


    // TODO: deal with field types (i.e. number sorting)
    public int CompareTo(RecordKey obj)
    {
        int pos = 0;
        int cur_result = 0;

        int thislen = key_parts.Count;
        int objlen = key_parts.Count;

        while (cur_result == 0)  // while equal
        {
            if (((thislen - pos) == 0) &&
                 ((objlen - pos) == 0))
            {
                // equal and at the end
                return 0; // equal
            }
            if (((thislen - pos) == 0) &&
                 ((objlen - pos) > 0))
            {
                // equal and obj longer
                return -1; // obj longer, so obj is greater
            }
            if (((thislen - pos) > 0) &&
                 ((objlen - pos) == 0))
            {
                // equal and this longer
                return 1; // this longer, so this greater
            }
            cur_result = this.key_parts[pos].CompareTo(obj.key_parts[pos]);
            pos++; // consider the next keypart
        }


        return cur_result;
    }

    public string DebugToString()
    {
        String srep = "K(";
        foreach (String part in key_parts)
        {
            srep += part + ":";
        }
        return srep + ")";
    }

    public override string ToString() {
        String srep = "";
        foreach (String part in key_parts) {
            srep += part + ":";
        }
        return srep;
    }


    // -----------------------------------------------------------
    // encoding/decoding of keyparts


    // decode
    void decode(byte[] data) {
        char[] delimiters = { ':' };
        System.Text.ASCIIEncoding enc = new System.Text.ASCIIEncoding();
        String keystring = enc.GetString(data);
        String[] keystring_parts = keystring.Split(delimiters);
        
        key_parts.AddRange(keystring_parts);
    }

    // encode
    public byte[] encode() {
        String srep = String.Join(":", key_parts.ToArray());

        System.Text.ASCIIEncoding enc = new System.Text.ASCIIEncoding();
        byte[] data = enc.GetBytes(srep);
        return data;
    }
}
}