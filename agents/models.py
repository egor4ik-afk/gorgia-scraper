from dataclasses import dataclass, field


@dataclass
class UpdateReport:
    total_scraped: int = 0
    new_products: int = 0
    updated_products: int = 0
    images_uploaded: int = 0
    price_changes: list = field(default_factory=list)
    stock_changes: list = field(default_factory=list)
    errors: list = field(default_factory=list)
