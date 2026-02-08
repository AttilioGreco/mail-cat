use serde::{Deserialize, Serialize};

/// Pagination parameters for list queries.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Pagination {
    pub page: u32,
    pub page_size: u32,
}

impl Pagination {
    pub fn new(page: u32, page_size: u32) -> Self {
        Self {
            page: page.max(1),
            page_size: page_size.clamp(1, 200),
        }
    }

    pub fn offset(&self) -> u64 {
        ((self.page - 1) * self.page_size) as u64
    }
}

impl Default for Pagination {
    fn default() -> Self {
        Self {
            page: 1,
            page_size: 50,
        }
    }
}

/// Paginated result container.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PagedResult<T> {
    pub items: Vec<T>,
    pub total_count: u64,
    pub page: u32,
    pub page_size: u32,
}

impl<T> PagedResult<T> {
    pub fn new(items: Vec<T>, total_count: u64, pagination: &Pagination) -> Self {
        Self {
            items,
            total_count,
            page: pagination.page,
            page_size: pagination.page_size,
        }
    }

    pub fn has_next_page(&self) -> bool {
        (self.page as u64 * self.page_size as u64) < self.total_count
    }

    pub fn total_pages(&self) -> u32 {
        if self.page_size == 0 {
            return 0;
        }
        ((self.total_count + self.page_size as u64 - 1) / self.page_size as u64) as u32
    }
}
