/// Observer implementation for arrays `[T; N]`.
pub mod array;
mod helper;
/// Observer implementation for [`LinkedList`](std::collections::LinkedList).
pub mod linked_list;
mod range_set;
/// Observer implementation for slices `[T]`.
pub mod slice;
/// Observer implementation for [`Vec`](std::vec::Vec).
pub mod vec;
/// Observer implementation for [`VecDeque`](std::collections::VecDeque).
pub mod vec_deque;

pub use array::ArrayObserver;
pub use linked_list::LinkedListObserver;
pub use slice::SliceObserver;
pub use vec::VecObserver;
pub use vec_deque::VecDequeObserver;
