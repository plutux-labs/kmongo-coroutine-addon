package com.plutux.mongo

import com.mongodb.MongoNamespace
import com.mongodb.async.SingleResultCallback
import com.mongodb.async.client.*
import com.mongodb.bulk.BulkWriteResult
import com.mongodb.client.model.*
import com.mongodb.client.result.DeleteResult
import com.mongodb.client.result.UpdateResult
import org.bson.Document
import org.bson.conversions.Bson
/**
 * Wrap the origin MongoCollection and apply default session to queries
 */
class SessionMongoCollection<T>(collection: MongoCollection<T>, val session: ClientSession) : MongoCollection<T> by collection {
    override fun findOneAndUpdate(filter: Bson, update: Bson, callback: SingleResultCallback<T>) {
        findOneAndUpdate(session, filter, update, callback)
    }

    override fun findOneAndUpdate(filter: Bson, update: Bson, options: FindOneAndUpdateOptions, callback: SingleResultCallback<T>) {
        findOneAndUpdate(session, filter, update, options, callback)
    }

    override fun findOneAndDelete(filter: Bson, callback: SingleResultCallback<T>) {
        findOneAndDelete(session, filter, callback)
    }

    override fun findOneAndDelete(filter: Bson, options: FindOneAndDeleteOptions, callback: SingleResultCallback<T>) {
        findOneAndDelete(session, filter, options, callback)
    }

    override fun findOneAndReplace(filter: Bson, replacement: T, callback: SingleResultCallback<T>) {
        findOneAndReplace(session, filter, replacement, callback)
    }

    override fun findOneAndReplace(filter: Bson, replacement: T, options: FindOneAndReplaceOptions, callback: SingleResultCallback<T>) {
        findOneAndReplace(session, filter, replacement, options, callback)
    }

    override fun updateMany(filter: Bson, update: Bson, callback: SingleResultCallback<UpdateResult>) {
        updateMany(session, filter, update, callback)
    }

    override fun updateMany(filter: Bson, update: Bson, options: UpdateOptions, callback: SingleResultCallback<UpdateResult>) {
        updateMany(session, filter, update, options, callback)
    }

    override fun updateOne(filter: Bson, update: Bson, callback: SingleResultCallback<UpdateResult>) {
        updateOne(session, filter, update, callback)
    }

    override fun updateOne(filter: Bson, update: Bson, options: UpdateOptions, callback: SingleResultCallback<UpdateResult>) {
        updateOne(session, filter, update, options, callback)
    }

    override fun find(filter: Bson): FindIterable<T> {
        return find(session, filter)
    }

    override fun find(): FindIterable<T> {
        return find(session)
    }

    override fun <TResult : Any?> find(filter: Bson, resultClass: Class<TResult>): FindIterable<TResult> {
        return find(session, filter, resultClass)
    }

    override fun <TResult : Any?> find(resultClass: Class<TResult>): FindIterable<TResult> {
        return find(session, resultClass)
    }

    override fun countDocuments(filter: Bson, callback: SingleResultCallback<Long>) {
        countDocuments(session, filter, callback)
    }

    override fun countDocuments(callback: SingleResultCallback<Long>) {
        countDocuments(session, callback)
    }

    override fun countDocuments(filter: Bson, options: CountOptions, callback: SingleResultCallback<Long>) {
        countDocuments(session, filter, options, callback)
    }

    override fun <TResult : Any?> distinct(fieldName: String, resultClass: Class<TResult>): DistinctIterable<TResult> {
        return distinct(session, fieldName, resultClass)
    }

    override fun <TResult : Any?> distinct(fieldName: String, filter: Bson, resultClass: Class<TResult>): DistinctIterable<TResult> {
        return distinct(session, fieldName, filter, resultClass)
    }

    override fun aggregate(pipeline: MutableList<out Bson>): AggregateIterable<T> {
        return aggregate(session, pipeline)
    }

    override fun <TResult : Any?> aggregate(pipeline: MutableList<out Bson>, resultClass: Class<TResult>): AggregateIterable<TResult> {
        return aggregate(session, pipeline, resultClass)
    }

    override fun watch(): ChangeStreamIterable<T> {
        return watch(session)
    }

    override fun watch(pipeline: MutableList<out Bson>): ChangeStreamIterable<T> {
        return watch(session, pipeline)
    }

    override fun <TResult : Any?> watch(pipeline: MutableList<out Bson>, resultClass: Class<TResult>): ChangeStreamIterable<TResult> {
        return watch(session, pipeline, resultClass)
    }

    override fun <TResult : Any?> watch(resultClass: Class<TResult>): ChangeStreamIterable<TResult> {
        return watch(session, resultClass)
    }

    override fun insertMany(documents: MutableList<out T>, callback: SingleResultCallback<Void>) {
        insertMany(session, documents, callback)
    }

    override fun insertMany(documents: MutableList<out T>, options: InsertManyOptions, callback: SingleResultCallback<Void>) {
        insertMany(session, documents, options, callback)
    }

    override fun insertOne(document: T, callback: SingleResultCallback<Void>) {
        insertOne(session, document, callback)
    }

    override fun insertOne(document: T, options: InsertOneOptions, callback: SingleResultCallback<Void>) {
        insertOne(session, document, options, callback)
    }

    override fun deleteMany(filter: Bson, callback: SingleResultCallback<DeleteResult>) {
        deleteMany(session, filter, callback)
    }

    override fun deleteMany(filter: Bson, options: DeleteOptions, callback: SingleResultCallback<DeleteResult>) {
        deleteMany(session, filter, options, callback)
    }

    override fun deleteOne(filter: Bson, callback: SingleResultCallback<DeleteResult>) {
        deleteOne(session, filter, callback)
    }

    override fun deleteOne(filter: Bson, options: DeleteOptions, callback: SingleResultCallback<DeleteResult>) {
        deleteOne(session, filter, options, callback)
    }

    override fun replaceOne(filter: Bson, replacement: T, callback: SingleResultCallback<UpdateResult>) {
        replaceOne(session, filter, replacement, callback)
    }

    override fun replaceOne(filter: Bson, replacement: T, options: ReplaceOptions, callback: SingleResultCallback<UpdateResult>) {
        replaceOne(session, filter, replacement, options, callback)
    }

    override fun bulkWrite(requests: MutableList<out WriteModel<out T>>, callback: SingleResultCallback<BulkWriteResult>) {
        bulkWrite(session, requests, callback)
    }

    override fun bulkWrite(requests: MutableList<out WriteModel<out T>>, options: BulkWriteOptions, callback: SingleResultCallback<BulkWriteResult>) {
        bulkWrite(session, requests, options, callback)
    }

    override fun createIndex(key: Bson, callback: SingleResultCallback<String>) {
        createIndex(session, key, callback)
    }

    override fun createIndex(key: Bson, options: IndexOptions, callback: SingleResultCallback<String>) {
        createIndex(session, key, options, callback)
    }

    override fun createIndexes(indexes: MutableList<IndexModel>, callback: SingleResultCallback<MutableList<String>>) {
        createIndexes(session, indexes, callback)
    }

    override fun createIndexes(indexes: MutableList<IndexModel>, createIndexOptions: CreateIndexOptions, callback: SingleResultCallback<MutableList<String>>) {
        createIndexes(session, indexes, createIndexOptions, callback)
    }

    override fun renameCollection(newCollectionNamespace: MongoNamespace, callback: SingleResultCallback<Void>) {
        renameCollection(session, newCollectionNamespace, callback)
    }

    override fun renameCollection(newCollectionNamespace: MongoNamespace, options: RenameCollectionOptions, callback: SingleResultCallback<Void>) {
        renameCollection(session, newCollectionNamespace, options, callback)
    }

    override fun drop(callback: SingleResultCallback<Void>) {
        drop(session, callback)
    }

    override fun dropIndex(indexName: String, callback: SingleResultCallback<Void>) {
        dropIndex(session, indexName, callback)
    }

    override fun dropIndex(keys: Bson, callback: SingleResultCallback<Void>) {
        dropIndex(session, keys, callback)
    }

    override fun dropIndex(indexName: String, dropIndexOptions: DropIndexOptions, callback: SingleResultCallback<Void>) {
        dropIndex(session, indexName, dropIndexOptions, callback)
    }

    override fun dropIndex(keys: Bson, dropIndexOptions: DropIndexOptions, callback: SingleResultCallback<Void>) {
        dropIndex(session, keys, dropIndexOptions, callback)
    }

    override fun dropIndexes(callback: SingleResultCallback<Void>) {
        dropIndexes(session, callback)
    }

    override fun dropIndexes(dropIndexOptions: DropIndexOptions, callback: SingleResultCallback<Void>) {
        dropIndexes(session, dropIndexOptions, callback)
    }

    override fun listIndexes(): ListIndexesIterable<Document> {
        return listIndexes(session)
    }

    override fun <TResult : Any?> listIndexes(resultClass: Class<TResult>): ListIndexesIterable<TResult> {
        return listIndexes(session, resultClass)
    }

    override fun mapReduce(mapFunction: String, reduceFunction: String): MapReduceIterable<T> {
        return mapReduce(session, mapFunction, reduceFunction)
    }

    override fun <TResult : Any?> mapReduce(mapFunction: String, reduceFunction: String, resultClass: Class<TResult>): MapReduceIterable<TResult> {
        return mapReduce(session, mapFunction, reduceFunction, resultClass)
    }

    override fun count(callback: SingleResultCallback<Long>) {
        return count(session, callback)
    }

    override fun count(filter: Bson, callback: SingleResultCallback<Long>) {
        return count(session, filter, callback)
    }

    override fun count(filter: Bson, options: CountOptions, callback: SingleResultCallback<Long>) {
        return count(session, filter, options, callback)
    }
}