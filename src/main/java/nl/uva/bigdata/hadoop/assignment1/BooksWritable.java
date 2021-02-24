package nl.uva.bigdata.hadoop.assignment1;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;

public class BooksWritable  implements Writable {

    private Book[] books;

    public void setBooks(Book[] books) {
        this.books = books;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(this.books.length);
        for (int i = 0; i < this.books.length; i = i + 1) {
            out.writeUTF(this.books[i].getTitle());
            out.writeInt(this.books[i].getYear());
        }
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        int bookCount = in.readInt();
        Book[] newBooks = new Book[bookCount];
        for (int i = 0; i < bookCount; i++) {
            String bookTitle = in.readUTF();
            int bookYear = in.readInt();
            newBooks[i] = new Book(bookTitle, bookYear);
        }
        this.setBooks(newBooks);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        BooksWritable that = (BooksWritable) o;
        return Arrays.equals(books, that.books);
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(books);
    }
}
