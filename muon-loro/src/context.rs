use kernel::{
    Change, Collect, Here, Observe, OwnedPath, Path, PathSegment, Query, Replace, Scope, Zero,
};
use loro::{
    Container, ContainerTrait, ExportMode, IntoContainerId, LoroCounter, LoroDoc, LoroList,
    LoroMap, LoroMovableList, LoroText, ToJson, ValueOrContainer, VersionVector,
};

use crate::Error;
use crate::materialize;

/// The attached Loro container representing the observed model.
///
/// Collection writes into Loro's current pending transaction. Call [`LoroDoc::commit`]
/// on the source document after successful collection to make the whole tracked block
/// one Loro change.
pub struct Context {
    root: Container,
}

/// A transaction staged on a fork of a [`Context`]'s Loro document.
pub struct Fork<'a> {
    source: &'a mut Context,
    context: Context,
    doc: LoroDoc,
    base: VersionVector,
}

/// A successfully collected fork waiting to be imported into its source document.
#[must_use = "a collected fork does not affect its source until it is imported"]
pub struct Staged<'a, T> {
    source: &'a mut Context,
    doc: LoroDoc,
    base: VersionVector,
    output: T,
}

impl Context {
    /// Uses an attached Loro container as the model root.
    pub fn new<ContainerType: ContainerTrait>(root: ContainerType) -> Result<Self, Error> {
        root.is_attached()
            .then(|| Self {
                root: root.to_container(),
            })
            .ok_or(Error::DetachedRoot)
    }

    /// Uses a named root map from a document as the model root.
    pub fn map<I: IntoContainerId>(doc: &LoroDoc, root: I) -> Self {
        Self {
            root: Container::Map(doc.get_map(root)),
        }
    }

    /// Returns the model's root container.
    pub fn root(&self) -> &Container {
        &self.root
    }

    /// Materializes a model as the current root value.
    ///
    /// This is the bootstrap counterpart to mutation collection: call it before the first
    /// tracked edit when the attached Loro root does not yet contain the model's container tree.
    /// The serialized form must fit Loro's value model: roots are containers, map keys are
    /// strings or primitive scalars, and integers fit in `i64`.
    pub fn initialize<T: serde::Serialize + ?Sized>(&self, value: &T) -> Result<(), Error> {
        materialize::replace(self, &Path::root(), value)
    }

    /// Restores a Rust model from the current deep value of the Loro root.
    ///
    /// The returned value is an unversioned projection. It must still represent this context's
    /// current root when passed to [`Context::collect`].
    pub fn hydrate<T: serde::de::DeserializeOwned>(&self) -> Result<T, Error> {
        let value = ValueOrContainer::Container(self.root.clone()).get_deep_value();
        serde_json::from_value(value.to_json_value()).map_err(Error::from)
    }

    /// Runs a tracked body and delivers its observations into this Loro context.
    ///
    /// The observed model must represent the current root when collection begins, and the root
    /// must not be changed independently until collection finishes. Collection writes directly
    /// into the current pending transaction and can leave both the model and document partially
    /// changed on error. Use [`Context::fork`] when document-side atomicity is required.
    pub fn collect<Model, Selection, Body, Output, Routes>(
        &mut self,
        model: &mut Model,
        body: Body,
    ) -> Result<Output, Error>
    where
        Model: Observe<Model, Selection> + ?Sized,
        <Model as Observe<Model, Selection>>::Observer<Model, Zero>:
            Collect<Context, Routes, Error, Scope<(), ()>>,
        Body: FnOnce(&mut <Model as Observe<Model, Selection>>::Observer<Model, Zero>) -> Output,
    {
        kernel::collect(model, body, self)
    }

    /// Starts an isolated Loro transaction rooted at this context.
    ///
    /// The source document must not contain pending operations: otherwise a fork would implicitly
    /// commit work outside this transaction. Collection happens only on the fork; call
    /// [`Staged::import`] after successful collection to update the source document atomically.
    /// This does not roll back mutations already made to the observed Rust model if collection
    /// fails.
    pub fn fork(&mut self) -> Result<Fork<'_>, Error> {
        let doc = self.root.doc().ok_or(Error::DetachedRoot)?;
        let pending = doc.get_pending_txn_len();
        if pending != 0 {
            return Err(Error::PendingTransaction(pending));
        }

        let base = doc.oplog_vv();
        let staged_doc = doc.fork();
        let root = staged_doc
            .get_container(self.root.id())
            .ok_or(Error::MissingForkRoot)?;

        Ok(Fork {
            source: self,
            context: Context { root },
            doc: staged_doc,
            base,
        })
    }

    pub(crate) fn index(
        segment: &PathSegment,
        len: usize,
        path: &OwnedPath,
    ) -> Result<usize, Error> {
        match segment {
            PathSegment::Positive(index) => Ok(*index),
            PathSegment::Negative(back) => len
                .checked_sub(*back)
                .ok_or_else(|| Error::IndexOutOfBounds(path.clone())),
            PathSegment::String(_) => Err(Error::InvalidSegment(path.clone())),
            PathSegment::Identity(_) => Err(Error::UnsupportedIdentity(path.clone())),
        }
    }

    fn child(
        &self,
        parent: &Container,
        segment: &PathSegment,
        path: &OwnedPath,
    ) -> Result<ValueOrContainer, Error> {
        let child = match parent {
            Container::Map(map) => match segment {
                PathSegment::String(key) => map.get(key),
                PathSegment::Identity(_) => return Err(Error::UnsupportedIdentity(path.clone())),
                _ => return Err(Error::InvalidSegment(path.clone())),
            },
            Container::List(list) => list.get(Self::index(segment, list.len(), path)?),
            Container::MovableList(list) => list.get(Self::index(segment, list.len(), path)?),
            _ => return Err(Error::ExpectedContainer(path.clone())),
        };
        child.ok_or_else(|| Error::MissingPath(path.clone()))
    }

    fn container(value: ValueOrContainer, path: &OwnedPath) -> Result<Container, Error> {
        match value {
            ValueOrContainer::Container(container) => Ok(container),
            _ => Err(Error::ExpectedContainer(path.clone())),
        }
    }

    pub(crate) fn resolve_parent(
        &self,
        path: &Path<'_>,
    ) -> Result<(Container, PathSegment), Error> {
        let path = path.to_owned();
        let (last, parents) = path.split_last().ok_or(Error::EmptyPath)?;
        let mut parent = self.root.clone();
        for segment in parents {
            parent = Self::container(self.child(&parent, segment, &path)?, &path)?;
        }
        Ok((parent, last.clone()))
    }

    fn resolve_value(&self, path: &Path<'_>) -> Result<ValueOrContainer, Error> {
        let owned = path.to_owned();
        if owned.is_empty() {
            return Ok(ValueOrContainer::Container(self.root.clone()));
        }
        let (parent, last) = self.resolve_parent(path)?;
        self.child(&parent, &last, &owned)
    }

    pub(crate) fn resolve_text(&self, path: &Path<'_>) -> Result<LoroText, Error> {
        match self.resolve_value(path)? {
            ValueOrContainer::Container(Container::Text(text)) => Ok(text),
            _ => Err(Error::ExpectedText(path.to_owned())),
        }
    }

    pub(crate) fn resolve_counter(&self, path: &Path<'_>) -> Result<LoroCounter, Error> {
        match self.resolve_value(path)? {
            ValueOrContainer::Container(Container::Counter(counter)) => Ok(counter),
            _ => Err(Error::ExpectedCounter(path.to_owned())),
        }
    }

    pub(crate) fn resolve_list(&self, path: &Path<'_>) -> Result<LoroList, Error> {
        match self.resolve_value(path)? {
            ValueOrContainer::Container(Container::List(list)) => Ok(list),
            _ => Err(Error::ExpectedList(path.to_owned())),
        }
    }

    pub(crate) fn resolve_map(&self, path: &Path<'_>) -> Result<LoroMap, Error> {
        match self.resolve_value(path)? {
            ValueOrContainer::Container(Container::Map(map)) => Ok(map),
            _ => Err(Error::ExpectedMap(path.to_owned())),
        }
    }

    pub(crate) fn resolve_movable_list(&self, path: &Path<'_>) -> Result<LoroMovableList, Error> {
        match self.resolve_value(path)? {
            ValueOrContainer::Container(Container::MovableList(list)) => Ok(list),
            _ => Err(Error::ExpectedMovableList(path.to_owned())),
        }
    }
}

impl<'a> Fork<'a> {
    /// Runs a tracked body against the fork without changing the source document.
    pub fn collect<Model, Selection, Body, Output, Routes>(
        mut self,
        model: &mut Model,
        body: Body,
    ) -> Result<Staged<'a, Output>, Error>
    where
        Model: Observe<Model, Selection> + ?Sized,
        <Model as Observe<Model, Selection>>::Observer<Model, Zero>:
            Collect<Context, Routes, Error, Scope<(), ()>>,
        Body: FnOnce(&mut <Model as Observe<Model, Selection>>::Observer<Model, Zero>) -> Output,
    {
        let output = self.context.collect(model, body)?;
        Ok(self.stage(output))
    }

    /// Awaits a tracked body against the fork without changing the source document.
    pub async fn collect_async<Model, Selection, Body, Output, Routes>(
        mut self,
        model: &mut Model,
        body: Body,
    ) -> Result<Staged<'a, Output>, Error>
    where
        Model: Observe<Model, Selection> + ?Sized,
        <Model as Observe<Model, Selection>>::Observer<Model, Zero>:
            Collect<Context, Routes, Error, Scope<(), ()>>,
        Body:
            AsyncFnOnce(&mut <Model as Observe<Model, Selection>>::Observer<Model, Zero>) -> Output,
    {
        let output = kernel::collect_async(model, body, &mut self.context).await?;
        Ok(self.stage(output))
    }

    fn stage<T>(self, output: T) -> Staged<'a, T> {
        self.doc.commit();
        Staged {
            source: self.source,
            doc: self.doc,
            base: self.base,
            output,
        }
    }
}

impl<T> Staged<'_, T> {
    /// Imports the fork's single committed transaction into the source document.
    pub fn import(self) -> Result<T, Error> {
        let update = self.doc.export(ExportMode::updates(&self.base))?;
        let source = self.source.root.doc().ok_or(Error::DetachedRoot)?;
        source.import(&update)?;
        Ok(self.output)
    }
}

impl Query<Context, Here> for Context {
    type Output = Self;

    fn query(&mut self) -> &mut Self::Output {
        self
    }
}

impl<'a, T: serde::Serialize + ?Sized, Before: ?Sized> Query<Change<'a, T, Before>, Here>
    for Context
{
    type Output = Self;

    fn query(&mut self) -> &mut Self::Output {
        self
    }
}

impl<T: serde::Serialize + ?Sized, Before: ?Sized> Replace<T, Before> for Context {
    type Error = Error;

    fn replace(
        &mut self,
        path: &Path<'_>,
        _: Option<&Before>,
        after: &T,
    ) -> Result<(), Self::Error> {
        materialize::replace(self, path, after)
    }
}
