//
//  RDNationalAccountsDataLayerSqlite.swift
//  RouteDriver
//
//  Created by Vasilij Orlov on 9/28/17.
//  Copyright © 2017 Stylesoft. All rights reserved.
//
import UIKit

class RDNationalAccountsDataLayerSqlite: NSObject, RDNationalAccountsDataStore {
    
    fileprivate let  database = RDDatabase.sharedInstance()
    
    
    func allNationalAccounts() -> [String:RDNationalAccount]?{
        if let mutex = database?.databaseMutex {
            return synchronized(lock: mutex) {
                let databaseOpened = database?.openReadOnly(true) ?? false
                
                guard databaseOpened else {
                    return nil
                }
                
                let db = database?.sqliteDatabase
                var na_stmt:OpaquePointer?
                let na_sql = String(format:"SELECT * FROM %s", Db_Table_NA)
                let state = sqlite3_prepare_v2(db, na_sql, -1, &na_stmt, nil)
                
                if(state != SQLITE_OK){
                    print("Error in preparing statement for sql %s, err: %s", na_sql, sqlite3_errmsg(db))
                }
                
                let accountsDict = getNationalAccountsWithStatement(stmt: na_stmt)
                
                sqlite3_finalize(na_stmt)
                database?.close()
                return accountsDict
            }
        }
        return nil
    }
    
 
    fileprivate func getNationalAccountsWithStatement(stmt:OpaquePointer?) -> [String:RDNationalAccount]?{
        var result = [String:RDNationalAccount]()
        while sqlite3_step(stmt) == SQLITE_ROW {
            let fieldNumber = sqlite3_column_count(stmt)
            let account = RDNationalAccount()
            for i in 0..<fieldNumber{
                let cn: UnsafePointer<CChar> = sqlite3_column_name(stmt, i)
                if strcmp(cn, Db_Field_Id) == 0 {
                    account.primaryId = Int(sqlite3_column_int(stmt, i))
                } else if strcmp(cn, Db_Field_NA_Id) == 0 {
                    account.systemId = sqlite3_column_int(stmt, i)
                }else if strcmp(cn, Db_Field_NA_SourceId) == 0 {
                    account.sourceId = sqlite3_column_int(stmt, i)
                }
            }
            result[account.keyForHashMap()] = account
        }
        return result.capacity == 0 ? nil : result
    }
}

//MARK: - extension

extension NSObject {
    func synchronized<T>( lock:AnyObject, block:() throws -> T ) rethrows -> T {
        objc_sync_enter(lock)
        defer {
            objc_sync_exit(lock)
        }
        return try block()
    }
}


