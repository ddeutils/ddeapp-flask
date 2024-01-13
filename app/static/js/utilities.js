function capitalize(string) {
    return string.charAt(0).toUpperCase() + string.slice(1);
};


function just_sleep(milliseconds) {
    const date = Date.now();
    let currentDate = null;
    do {
      currentDate = Date.now();
    } while (currentDate - date < milliseconds);
}


function sleep(ms) {
    return new Promise(resolve => setTimeout(resolve, ms));
}

async function delayedGreeting() {
    console.log("Hello");
    await sleep(2000);
    console.log("World!");
    await sleep(2000);
    console.log("Goodbye!");
}
