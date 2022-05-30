import Foundation
import CoreData

public class CoreDataHybridtorage: Storage {
    
    // MARK: - Attributes
    
    internal let store: CoreDataStore
    internal var objectModel: NSManagedObjectModel! = nil
    internal var persistentStore: NSPersistentStore! = nil
    internal var persistentStoreCoordinator: NSPersistentStoreCoordinator! = nil
    internal var rootSavingContext: NSManagedObjectContext! = nil
    internal var persistenceContainer: NSPersistentContainer! = nil
    
    // MARK: - Storage conformance
    
    public var description: String {
        get {
            return "CoreDataDefaultStorage"
        }
    }
    
    public var type: StorageType = .coreData
    public var mainContext: Context!
    private var _saveContext: Context!
    public var saveContext: Context! {
        if let context = self._saveContext {
            return context
        }
        let _context = cdContext(withParent: .context(self.rootSavingContext), concurrencyType: .privateQueueConcurrencyType, inMemory: false)
        _context.observe(inMainThread: true) { [weak self] (notification) -> Void in
            (self?.mainContext as? NSManagedObjectContext)?.mergeChanges(fromContextDidSave: notification as Notification)
        }
        self._saveContext = _context
        return _context
    }
    public var memoryContext: Context! {
        let _context =  cdContext(withParent: .context(self.rootSavingContext), concurrencyType: .privateQueueConcurrencyType, inMemory: true)
        return _context
    }
    
    public func operation<T>(_ operation: @escaping (_ context: Context, _ save: @escaping () -> Void) throws -> T) throws -> T {
        let context: NSManagedObjectContext = self.saveContext as! NSManagedObjectContext
        var _error: Error!
        
        var returnedObject: T!
        context.performAndWait {
            do {
                returnedObject = try operation(context, { () -> Void in
                    do {
                        try context.save()
                    }
                    catch {
                        _error = error
                    }
                    self.rootSavingContext.performAndWait({
                        if self.rootSavingContext.hasChanges {
                            do {
                                try self.rootSavingContext.save()
                            }
                            catch {
                                _error = error
                            }
                        }
                    })
                })
            } catch {
                _error = error
            }
        }
        if let error = _error {
            throw error
        }
        
        return returnedObject
    }
    
    public func backgroundOperation(_ operation: @escaping (_ context: Context, _ save: @escaping () -> Void) -> (), completion: @escaping (Error?) -> ()) {
        let context: NSManagedObjectContext = self.saveContext as! NSManagedObjectContext
        var _error: Error!
        context.perform {
            operation(context, { () -> Void in
                do {
                    try context.save()
                }
                catch {
                    _error = error
                }
                self.rootSavingContext.perform {
                    if self.rootSavingContext.hasChanges {
                        do {
                            try self.rootSavingContext.save()
                        }
                        catch {
                            _error = error
                        }
                    }
                    completion(_error)
                }
            })
        }
    }

    public func removeStore() throws {
        try FileManager.default.removeItem(at: store.path() as URL)
        _ = try? FileManager.default.removeItem(atPath: "\(store.path().absoluteString)-shm")
        _ = try? FileManager.default.removeItem(atPath: "\(store.path().absoluteString)-wal")

    }
    
    
    // MARK: - Init
    
    public init(persistenceContainer: NSPersistentContainer, migrate: Bool) throws {
        self.persistenceContainer = persistenceContainer
        self.store = CoreDataStore.named(persistenceContainer.name)
        self.objectModel = persistenceContainer.managedObjectModel
        self.persistentStoreCoordinator = persistenceContainer.persistentStoreCoordinator
        self.persistentStore = try cdInitializeStore(store: store, storeCoordinator: persistentStoreCoordinator, migrate: migrate)
        self.rootSavingContext = cdContext(withParent: .coordinator(self.persistenceContainer.persistentStoreCoordinator), concurrencyType: .privateQueueConcurrencyType, inMemory: false)
        self.mainContext = cdContext(withParent: .context(self.rootSavingContext), concurrencyType: .mainQueueConcurrencyType, inMemory: false)
    }
    
    
    // MARK: - Public
    
    @available(OSX 10.12, *)
    public func observable<T: NSManagedObject>(request: FetchRequest<T>) -> RequestObservable<T> where T:Equatable {
        return CoreDataObservable(request: request, context: self.mainContext as! NSManagedObjectContext)
    }
    
}

