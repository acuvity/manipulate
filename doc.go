// Copyright 2019 Aporeto Inc.
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//     http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Package manipulate provides everything needed to perform CRUD operations
// on an https://go.acuvity.ai/elemental based data model.
//
// The main interface is Manipulator. This interface provides various
// methods for creation, modification, retrieval and so on. TransactionalManipulator,
// which is an extension of the Manipulator add methods to manage transactions, like
// Commit and Abort.
//
// A Manipulator works with some elemental.Identifiables.
//
// The storage engine used by a Manipulator is abstracted. By default manipulate
// provides implementations for Rest API over HTTP or websocket, Mongo DB, Memory and a mock Manipulator for
// unit testing. You can of course create your own implementation.
//
// Each method of a Manipulator is taking a manipulate.Context as argument. The context is used
// to pass additional informations like a Filter or some Parameters.
//
// Mongo-specific helpers in manipmongo now use official-driver MongoDB types.
// See docs/mongodb-driver-migration.md for the breaking helper signature,
// BSON, sharder, and timeout migration notes.
//
// Example for creating an object:
//
//	// Create a User from a generated Elemental model.
//	user := models.NewUser()
//	user.FullName, user.Login := "Antoine Mercadal", "primalmotion"
//
//	// Create Mongo manipulator.
//	m, err := manipmongo.New("mongodb://db-username:db-password@127.0.0.1:27017/test?authSource=db-authsource", "test")
//	if err != nil {
//	    panic(err)
//	}
//
//	// Then create the User.
//	if err := m.Create(nil, user); err != nil {
//	    panic(err)
//	}
//
// Example for retrieving an object:
//
//	// Create a Context with a filter.
//	ctx := manipulate.NewContext(
//	    context.Background(),
//	    manipulate.ContextOptionFilter(
//	        elemental.NewFilterComposer().WithKey("login").Equals("primalmotion").Done(),
//	    ),
//	)
//
//	// Retrieve the users matching the filter.
//	var users models.UserLists
//	if err := m.RetrieveMany(ctx, &users); err != nil {
//	    panic(err)
//	}
package manipulate // import "go.acuvity.ai/manipulate"
