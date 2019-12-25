/*
 * Copyright 2018 Google
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include "Firestore/core/src/firebase/firestore/model/document.h"

#include <ostream>
#include <sstream>
#include <unordered_map>
#include <utility>

#include "Firestore/core/src/firebase/firestore/nanopb/nanopb_util.h"
#include "Firestore/core/src/firebase/firestore/nanopb/reader.h"
#include "Firestore/core/src/firebase/firestore/util/hard_assert.h"

namespace firebase {
namespace firestore {
namespace model {
namespace {

using nanopb::MakeStringView;
using nanopb::Message;
using nanopb::StringReader;

const google_firestore_v1_Value* FindTopField(
    const google_firestore_v1_Document& doc, const std::string& segment) {
  for (pb_size_t i = 0; i < doc.fields_count; i++) {
    const auto& entry = doc.fields[i];
    if (MakeStringView(entry.key) == segment) {
      return &entry.value;
    }
  }
  return nullptr;
}

const google_firestore_v1_Value* FindNestedField(
    const google_firestore_v1_MapValue& map, const std::string& segment) {
  for (pb_size_t i = 0; i < map.fields_count; i++) {
    const auto& entry = map.fields[i];
    if (MakeStringView(entry.key) == segment) {
      return &entry.value;
    }
  }
  return nullptr;
}

}  // namespace

static_assert(
    sizeof(MaybeDocument) == sizeof(Document),
    "Document may not have additional members (everything goes in Rep)");

class Document::Rep : public MaybeDocument::Rep {
 public:
  Rep(DocumentKey&& key,
      SnapshotVersion version,
      DocumentState document_state,
      ObjectValue&& data)
      : MaybeDocument::Rep(Type::Document, std::move(key), version),
        document_state_(document_state),
        data_(std::move(data)) {
  }

  Rep(DocumentKey&& key,
      SnapshotVersion version,
      DocumentState document_state,
      nanopb::Message<google_firestore_v1_Document> proto,
      FieldConverter converter)
      : MaybeDocument::Rep(Type::Document, std::move(key), version),
        document_state_(document_state),
        proto_(std::move(proto)),
        converter_(std::move(converter)) {
  }

  const ObjectValue& data() const {
    if (!data_.has_value()) {
      HARD_ASSERT(proto_.get() && converter_,
                  "Expected proto and converter to be set");

      StringReader reader(nullptr, 0);
      ObjectValue result;
      for (pb_size_t i = 0; i < proto_->fields_count; i++) {
        const auto& entry = proto_->fields[i];
        FieldPath path =
            FieldPath::FromSingleSegmentView(MakeStringView(entry.key));
        FieldValue value = converter_(&reader, entry.value);
        result = result.Set(path, value);
      }
      data_ = std::move(result);
    }

    return data_.value();
  }

  absl::optional<FieldValue> field(const FieldPath& path) const {
    if (data_) {
      return data_.value().Get(path);
    }

    std::lock_guard<std::mutex> lock(mutex_);
    auto found = field_value_cache_.find(path);
    if (found != field_value_cache_.end()) {
      return found->second;
    }

    const google_firestore_v1_Value* proto_value =
        FindTopField(*proto_, path.first_segment());
    for (size_t i = 1; proto_value != nullptr && i < path.size(); i++) {
      if (proto_value->which_value_type !=
          google_firestore_v1_Value_map_value_tag) {
        return absl::nullopt;
      }

      proto_value = FindNestedField(proto_value->map_value, path[i]);
    }

    if (proto_value == nullptr) {
      return absl::nullopt;
    }

    StringReader reader(nullptr, 0);
    FieldValue value = converter_(&reader, *proto_value);
    field_value_cache_[path] = value;

    return value;
  }

  DocumentState document_state() const {
    return document_state_;
  }

  bool has_local_mutations() const {
    return document_state_ == DocumentState::kLocalMutations;
  }

  bool has_committed_mutations() const {
    return document_state_ == DocumentState::kCommittedMutations;
  }

  bool has_pending_writes() const override {
    return has_local_mutations() || has_committed_mutations();
  }

  bool Equals(const MaybeDocument::Rep& other) const override {
    if (!MaybeDocument::Rep::Equals(other)) return false;

    const auto& other_rep = static_cast<const Rep&>(other);
    return document_state_ == other_rep.document_state_ &&
           data() == other_rep.data();
  }

  size_t Hash() const override {
    return util::Hash(MaybeDocument::Rep::Hash(), data(), document_state_);
  }

  std::string ToString() const override {
    return absl::StrCat("Document(key=", key().ToString(),
                        ", version=", version().ToString(),
                        ", document_state=", document_state_,
                        ", data=", data().ToString(), ")");
  }

 private:
  friend class Document;

  DocumentState document_state_;
  Message<google_firestore_v1_Document> proto_;
  FieldConverter converter_;
  mutable absl::optional<ObjectValue> data_;

  mutable std::mutex mutex_;
  mutable std::unordered_map<FieldPath, FieldValue, FieldPathHash>
      field_value_cache_;
};

Document::Document(DocumentKey key,
                   SnapshotVersion version,
                   DocumentState document_state,
                   ObjectValue data)
    : MaybeDocument(std::make_shared<Rep>(
          std::move(key), version, document_state, std::move(data))) {
}

Document::Document(DocumentKey key,
                   SnapshotVersion version,
                   DocumentState document_state,
                   nanopb::Message<google_firestore_v1_Document> proto,
                   FieldConverter converter)
    : MaybeDocument(std::make_shared<Rep>(std::move(key),
                                          version,
                                          document_state,
                                          std::move(proto),
                                          std::move(converter))) {
}

Document::Document(const MaybeDocument& document) : MaybeDocument(document) {
  HARD_ASSERT(type() == Type::Document);
}

const ObjectValue& Document::data() const {
  return doc_rep().data();
}

absl::optional<FieldValue> Document::field(const FieldPath& path) const {
  return doc_rep().field(path);
}

DocumentState Document::document_state() const {
  return doc_rep().document_state_;
}

bool Document::has_local_mutations() const {
  return doc_rep().has_local_mutations();
}

bool Document::has_committed_mutations() const {
  return doc_rep().has_committed_mutations();
}

const Message<google_firestore_v1_Document>& Document::proto() const {
  return doc_rep().proto_;
}

const Document::Rep& Document::doc_rep() const {
  return static_cast<const Rep&>(MaybeDocument::rep());
}

std::ostream& operator<<(std::ostream& os, DocumentState state) {
  switch (state) {
    case DocumentState::kCommittedMutations:
      return os << "kCommittedMutations";
    case DocumentState::kLocalMutations:
      return os << "kLocalMutations";
    case DocumentState::kSynced:
      return os << "kLocalSynced";
  }

  UNREACHABLE();
}

std::ostream& operator<<(std::ostream& os, const Document& doc) {
  return os << doc.doc_rep().ToString();
}

/** Compares against another Document. */
bool operator==(const Document& lhs, const Document& rhs) {
  return lhs.doc_rep().Equals(rhs.doc_rep());
}

}  // namespace model
}  // namespace firestore
}  // namespace firebase
