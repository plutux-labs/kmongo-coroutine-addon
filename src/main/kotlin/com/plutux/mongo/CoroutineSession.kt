package com.plutux.mongo

import com.mongodb.async.client.ClientSession
import com.mongodb.async.client.MongoCollection
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.withContext
import org.litote.kmongo.coroutine.abortTransaction
import org.litote.kmongo.coroutine.commitTransaction
import kotlin.coroutines.CoroutineContext

/**
 * Put ClientSession in the coroutin context
 */

val SESSION_KEY = object : CoroutineContext.Key<SessionElement> {}

class SessionElement(val clientSession: ClientSession) : CoroutineContext.Element {
    override val key: CoroutineContext.Key<*>
        get() = SESSION_KEY
}

suspend fun <T> MongoCollection<T>.withCoroutineSession() =
    coroutineScope { coroutineContext[SESSION_KEY] }
        ?.clientSession
        .let {
            if (it != null) {
                SessionMongoCollection(this, it)
            } else this
        }

suspend fun <T> ClientSession.useCoroutine(fn: suspend CoroutineScope.() -> T): T {
    return withContext(SessionElement(this)) {
        startTransaction()
        try {
            val t = fn(this)
            commitTransaction()
            t
        } catch (e: Exception) {
            abortTransaction()
            throw e
        }
    }
}