from flask import Flask, request, render_template_string
import os
import re
import multiprocessing

app = Flask(__name__)

# Define mapper function
def mapper(data_chunk):
    word_counts = {}
    # Split the input data chunk into words using regular expressions
    words = re.findall(r'\w+', data_chunk.lower())
    # Count the number of occurrences of each word in the data chunk
    for word in words:
        if word in word_counts:
            word_counts[word] += 1
        else:
            word_counts[word] = 1
    return word_counts

# Define reducer function
def reducer(results):
    word_count = {}
    # Merge the word counts from each data chunk
    for result in results:
        for word, count in result.items():
            if word in word_count:
                word_count[word] += count
            else:
                word_count[word] = count
    return word_count

@app.route('/', methods=['GET', 'POST'])
def word_count():
    if request.method == 'POST':
        # Save uploaded file to the local disk
        f = request.files['file']
        filename = f.filename
        f.save(filename)

        # Read data from the file and split into chunks for mapping
        with open(filename, 'r') as f:
            data = f.read()
        data_chunks = [chunk for chunk in data.split(os.linesep) if chunk]

        # Create a pool of workers for mapping
        pool = multiprocessing.Pool()

        # Map data chunks to workers
        results = pool.map(mapper, data_chunks)

        # Reduce results
        word_count = reducer(results)

        # Sort word counts by frequency in descending order
        sorted_word_count = sorted(word_count.items(), key=lambda x: x[1], reverse=True)

        # Render the results template with the word counts
        return render_template_string('''
<!doctype html>
<html>
<head>
    <title>Word Count Results</title>
</head>
<body>
    <h1>Word Count Results</h1>
    <table>
        {% for word, count in sorted_word_count %}
        <tr>
            <td>{{ word }}</td>
            <td>{{ count }}</td>
        </tr>
        {% endfor %}
    </table>
</body>
</html>
''', sorted_word_count=sorted_word_count)

    # Render the upload form
    return render_template_string('''
<!doctype html>
<html>
<head>
    <title>Upload a Text File</title>
</head>
<body>
    <h1>Upload a Text File</h1>
    <form method="post" enctype="multipart/form-data">
        <input type="file" name="file">
        <input type="submit" value="Upload">
    </form>
</body>
</html>
''')

if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=80)
