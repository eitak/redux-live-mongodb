import {EventEmitter} from 'events'
import _ from 'lodash'
import hash from 'object-hash'
import MongoClient from 'mongodb'
import util from 'util'

const NEW_ACTION_EVENT = 'new-action';

export default class {

    constructor(url, actionCollectionName = 'actions', snapshotCollectionName = 'snapshots') {
        this.url = url;
        this.actionCollectionName = actionCollectionName;
        this.snapshotCollectionName = snapshotCollectionName;
        this._eventEmitter = new EventEmitter();
    }

    async connect() {
        const db = await MongoClient.connect(this.url);
        this.actionCollection = db.collection(this.actionCollectionName);
        this.snapshotCollection = db.collection(this.snapshotCollectionName);

        this.actionCollection
            .find({'reduxLive.timestamp': {$gt: Date.now()}})
            .addCursorFlag('tailable', true)
            .on('data', action => {
                var censoredAction = censorDocument(action);
                this._eventEmitter.emit(NEW_ACTION_EVENT, censoredAction);
                this._eventEmitter.emit(NEW_ACTION_EVENT + hash(action.reduxLive.streamId), censoredAction);
            });

        console.log('Connected to Redux Live MongoDB');
    }

    close() {
        this.db.close()
    }

    deleteStream(streamId) {
        return this.actionCollection.deleteOne({_id: hash(streamId)})
    }

    async createStream(streamId, initialState = {}) {
        const snapshotKey = hash(streamId);

        const snapshots = await this.snapshotCollection.find({_id: snapshotKey}).toArray();
        if (snapshots.length > 0) {
            throw new Error(util.format('State already exists for state ID %j', streamId));
        }

        var snapshot = {
            ...initialState,
            _id: snapshotKey,
            reduxLive: {
                sequenceNumber: 0,
                streamId: streamId
            }
        };
        await this.snapshotCollection.insertOne(snapshot);
    }

    async getSnapshot(streamId) {
        const snapshotKey = hash(streamId);
        const snapshots = await this.snapshotCollection.find({_id: snapshotKey}).toArray();
        if (snapshots.length === 0) {
            throw new Error(util.format('No state for stream ID %j', streamId));
        }

        return censorDocument(snapshots[0])
    }

    async saveSnapshot(snapshot) {
        const {streamId, sequenceNumber} = snapshot.reduxLive;

        const snapshotKey = hash(streamId);
        var snapshotToSave = {...snapshot, _id: snapshotKey};
        await this.snapshotCollection.updateOne(
            {_id: snapshotKey, 'reduxLive.sequenceNumber': sequenceNumber - 1},
            snapshotToSave);
    }

    async getAction(streamId, sequenceNumber) {
        const actionKey = hash({streamId, sequenceNumber});

        const actions = await this.actionCollection.find({_id: actionKey}).toArray();
        if (actions.length === 0) {
            throw new Error(util.format('No action for stream ID %j and sequence number %j', streamId, sequenceNumber))
        }

        return censorDocument(actions[0])
    }

    async saveAction(action) {
        const {streamId, sequenceNumber} = action.reduxLive;
        const actionKey = hash({streamId, sequenceNumber});
        await this.actionCollection.insertOne({...action, _id: actionKey});
    }

    onNewAction(cb) {
        this._eventEmitter.on(NEW_ACTION_EVENT, action => {
            cb(action)
        })
    }

    onNewActionFromStream(streamId, cb) {
        this._eventEmitter.on(NEW_ACTION_EVENT + hash(streamId), action => {
            cb(action)
        })
    }

}

function censorDocument(document) {
    return _.omit(document, '_id')
}
