using System;
using System.Collections.Generic;
using System.Text;
using System.Data.SqlClient;
using System.Data;

namespace Platform.Utility
{
    // .net 3.0 required for this file only rest of LRUCache can run in 2.0
    public class UserData
    {
        public int UserID { get; set; }
        public string UserName { get; set; }
        public string FirstName { get; set; }
        public string LastName { get; set; }
        public string Phone { get; set; }
    }

    public class UserCache : LRUCache<UserData>
    {
        private static UserCache instance = null;
        private IIndex<int> _findByUserID = null;
        private IIndex<string> _findByUserName = null;
        private long _tableVersion = 0;

        /// <summary>Singleton pattern forces everyone to share the cache</summary>
        public static UserCache Instance
        {
            get
            {
                if( instance == null )
                    lock( typeof( UserCache ) )
                        if( instance == null )
                            instance = new UserCache();
                return instance;
            }
        }

        /// <summary>retrieve items by userid</summary>
        public UserData FindByUserID( int userid )
        {
            return _findByUserID[userid];
        }

         /// <summary>retrieve items by username</summary>
       public UserData FindByUserName( string username )
        {
            return _findByUserName[username];
        }

        /// <summary>constructor creates cache and multiple indexes</summary>
        private UserCache()
            : base( 10000, TimeSpan.FromMinutes( 1 ), TimeSpan.FromHours( 1 ), null )
        {
            _isValid = IsDataValid;
            _findByUserID = AddIndex<int>( "UserID", delegate( UserData user ) { return user.UserID; }, LoadFromUserID );
            _findByUserName = AddIndex<string>( "UserName", delegate( UserData user ) { return user.UserName; }, LoadFromUserName );
            IsDataValid();
        }

        private delegate DataType LoadData<DataType>(IDataRecord reader);

        /// <summary>This data access is ugly but simple, didn't want to complicate things by including in my db wrapper classes</summary>
        private DataType GetDataRow<DataType>( string sql, object arg1, LoadData<DataType> loadFunc ) where DataType : class
        {
            using( SqlConnection conn = new SqlConnection( "" ) )
            {
                conn.Open();
                using( SqlCommand cmd = new SqlCommand( sql, conn ) )
                {
                    cmd.Parameters.AddWithValue( "@arg1", arg1 );
                    using( SqlDataReader reader = cmd.ExecuteReader() )
                        return (reader.Read() ? loadFunc( reader ) : null);
                }
            }
        }

        /// <summary>check to see if users table has changed, if so dump cache and reload.</summary>
        /// <remarks>If this query doesnt work on your sql2005 server, user privileges are too low</remarks>
        private bool IsDataValid()
        {
            long oldVersion = _tableVersion;
            string sql = @"select sum(user_updates) from sys.dm_db_index_usage_stats with(nolock) where object_id=OBJECT_ID(N'dbo.Users')";
            _tableVersion = (long?)GetDataRow<object>( sql, 1, delegate( IDataRecord dr ) {
                return dr.GetInt64( 1 );
            } ) ?? 0;
            return (oldVersion == _tableVersion);
        }

        /// <summary>when FindByUserID can't find a user, this method loads the data from the db</summary>
        private UserData LoadFromUserID( int userid )
        {
            string sql = @"select UserName, FirstName, LastName, Phone from dbo.users with(nolock) where UserID=@arg1";
            return GetDataRow<UserData>( sql, userid, delegate( IDataRecord dr ) {
                return new UserData { UserID = userid, UserName = dr.GetString( 1 ), FirstName = dr.GetString( 2 ), LastName = dr.GetString( 3 ), Phone = dr.GetString( 5 ) };
            } );
        }

        /// <summary>when FindByUserName can't find a user, this method loads the data from the db</summary>
        private UserData LoadFromUserName( string username )
        {
            string sql = @"select UserID, FirstName, LastName, Phone from dbo.users with(nolock) where UserName=@arg1";
            return GetDataRow<UserData>( sql, username, delegate( IDataRecord dr ) {
                return new UserData { UserID = dr.GetInt32(1), UserName = username, FirstName = dr.GetString( 2 ), LastName = dr.GetString( 3 ), Phone = dr.GetString( 5 ) };
            } );
         }
    }
}
