package com.plutux.mongo

import com.mongodb.MongoNamespace
import com.mongodb.async.SingleResultCallback
import com.mongodb.async.client.ClientSession
import com.mongodb.async.client.MongoCollection
import com.mongodb.async.client.MongoIterable
import com.mongodb.client.model.*
import com.mongodb.client.result.DeleteResult
import com.mongodb.client.result.UpdateResult
import org.bson.conversions.Bson
import org.litote.kmongo.coroutine.singleResult
import kotlin.coroutines.resume
import kotlin.coroutines.resumeWithException
import kotlin.coroutines.suspendCoroutine
import kotlin.reflect.KProperty

suspend inline fun <T> singleNonNullResult(crossinline callback: (SingleResultCallback<T>) -> Unit): T {
    return suspendCoroutine { continuation ->
        callback(SingleResultCallback { result: T, throwable: Throwable? ->
            if (throwable != null) {
                continuation.resumeWithException(throwable)
            } else {
                continuation.resume(result)
            }
        })
    }
}

suspend inline fun <reified T> MongoCollection<T>.estimateCount(
    options: EstimatedDocumentCountOptions = EstimatedDocumentCountOptions()
): Long = singleNonNullResult {
    estimatedDocumentCount(options, it)
}

suspend inline fun <reified T> MongoCollection<T>.insertOne(
    document: T,
    options: InsertOneOptions = InsertOneOptions(),
    session: ClientSession? = null
) = singleNonNullResult<Void> {
    if (session != null)
        insertOne(session, document, options, it)
    else
        insertOne(document, options, it)
}

suspend inline fun <reified T> MongoCollection<T>.insertMany(
    document: List<T>,
    options: InsertManyOptions = InsertManyOptions(),
    session: ClientSession? = null
) = singleNonNullResult<Void> {
    if (session != null)
        insertMany(session, document, options, it)
    else
        insertMany(document, options, it)
}

suspend inline fun <reified T> MongoCollection<T>.findOneAndReplace(
    filter: Bson,
    replacement: T,
    options: FindOneAndReplaceOptions = FindOneAndReplaceOptions(),
    session: ClientSession? = null
): T? = singleResult {
    if (session != null)
        findOneAndReplace(session, filter, replacement, options, it)
    else
        findOneAndReplace(filter, replacement, options, it)
}

suspend inline fun <reified T> MongoCollection<T>.findOneAndUpdate(
    filter: Bson,
    update: Bson,
    options: FindOneAndUpdateOptions = FindOneAndUpdateOptions(),
    session: ClientSession? = null
): T? = singleResult {
    if (session != null)
        findOneAndUpdate(session, filter, update, options, it)
    else
        findOneAndUpdate(filter, update, options, it)
}

suspend inline fun <reified T> MongoCollection<T>.findOneAndDelete(
    filter: Bson,
    options: FindOneAndDeleteOptions = FindOneAndDeleteOptions(),
    session: ClientSession? = null
): T? = singleResult {
    if (session != null)
        findOneAndDelete(session, filter, options, it)
    else
        findOneAndDelete(filter, options, it)
}

suspend inline fun <reified T> MongoCollection<T>.countDocuments(
    filter: Bson,
    options: CountOptions = CountOptions(),
    session: ClientSession? = null
): Long = singleNonNullResult {
    if (session != null)
        countDocuments(session, filter, options, it)
    else
        countDocuments(filter, options, it)
}

suspend inline fun <reified T> MongoCollection<T>.updateMany(
    filter: Bson,
    update: Bson,
    options: UpdateOptions = UpdateOptions(),
    clientSession: ClientSession? = null
) =
    singleNonNullResult<UpdateResult> {
        if (clientSession != null)
            updateMany(clientSession, filter, update, options, it)
        else
            updateMany(filter, update, options, it)
    }

suspend inline fun <reified T> MongoCollection<T>.updateOne(
    filter: Bson,
    update: Bson,
    options: UpdateOptions = UpdateOptions(),
    session: ClientSession? = null
) =
    singleNonNullResult<UpdateResult> {
        if (session != null)
            updateOne(session, filter, update, options, it)
        else
            updateOne(filter, update, options, it)
    }

suspend inline fun <reified T> MongoCollection<T>.deleteMany(
    filter: Bson,
    options: DeleteOptions = DeleteOptions(),
    session: ClientSession? = null
) =
    singleNonNullResult<DeleteResult> {
        if (session != null)
            deleteMany(session, filter, options, it)
        else
            deleteMany(filter, options, it)
    }

suspend inline fun <reified T> MongoCollection<T>.deleteOne(
    filter: Bson,
    options: DeleteOptions = DeleteOptions(),
    session: ClientSession? = null
) =
    singleNonNullResult<DeleteResult> {
        if (session != null)
            deleteOne(session, filter, options, it)
        else
            deleteOne(filter, options, it)
    }

suspend inline fun <reified T> MongoCollection<T>.replaceOne(
    filter: Bson,
    replacement: T,
    options: ReplaceOptions = ReplaceOptions(),
    session: ClientSession? = null
) =
    singleNonNullResult<UpdateResult> {
        if (session != null)
            replaceOne(session, filter, replacement, options, it)
        else
            replaceOne(filter, replacement, options, it)
    }

suspend inline fun <reified T> MongoCollection<T>.drop(session: ClientSession? = null) =
    singleNonNullResult<Void> {
        if (session != null)
            drop(session, it)
        else
            drop(it)
    }

suspend inline fun <reified T> MongoCollection<T>.createIndex(
    key: Bson,
    options: IndexOptions = IndexOptions(),
    session: ClientSession? = null
) =
    singleNonNullResult<String> {
        if (session != null)
            createIndex(session, key, options, it)
        else
            createIndex(key, options, it)
    }

suspend inline fun <reified T> MongoCollection<T>.createIndex(
    key: Bson,
    options: DropIndexOptions = DropIndexOptions(),
    session: ClientSession? = null
) =
    singleNonNullResult<Void> {
        if (session != null)
            dropIndex(session, key, options, it)
        else
            dropIndex(key, options, it)
    }

suspend inline fun <reified T> MongoCollection<T>.renameCollection(
    namespace: MongoNamespace,
    options: RenameCollectionOptions = RenameCollectionOptions(),
    session: ClientSession? = null
) =
    singleNonNullResult<Void> {
        if (session != null)
            renameCollection(session, namespace, options, it)
        else
            renameCollection(namespace, options, it)
    }

suspend inline fun <reified T> MongoIterable<T>.first(): T? = singleResult { first(it) }

infix fun <T> KProperty<T>.set(value: T): Bson = Updates.set(this.name, value)

infix fun <T : Set<R>, R> KProperty<T>.addToSet(value: R): Bson = Updates.addToSet(this.name, value)

infix fun <T> KProperty<T>.setOnInsert(value: T): Bson = Updates.setOnInsert(this.name, value)